# tventi is a tiny venti server.
# it keeps all scores in memory, so all lookups including writing duplicate blocks are fast.
#
# an index file is a sequence of block headers containing: score, type, size.
# a data file is a sequence of tuples made of a block header and the data.
# at startup, the index file is read and all scores placed in memory.
#
# serve() is in charge of the score index.
# it handles up to Ntokens=256 requests concurrently.
# each client is served by a proto() thread.

implement Tventi;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "dial.m";
	dial: Dial;
include "keyring.m";
	kr: Keyring;
include "venti.m";
	venti: Venti;
	Vmsg, Score: import venti;

Tventi: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

dflag: int;
cflag: int;
tflag: int;
Vflag: int;
index := "tindex";
data := "tdata";
addr := "*";
ioff,
doff: big;
ifd,
dfd: ref Sys->FD;

Ntokens: con 256;

R: adt {
	# req
	m:	ref Vmsg;
	s:	Score;

	# resp
	n:	int;
	o:	big;
	err:	string;
};
zeror: R;

lookupc: chan of (Score, int, R, chan of R);
putc: chan of (Score, int, array of byte, R, chan of R);
syncc: chan of (R, chan of R);

Isize: con 1+20+2;
I: adt {
	t:	int;		# type
	s:	Score;		# score
	n:	int;		# size
	o:	big;		# offset of I in data file.  not on disk.

	unpack:	fn(d: array of byte, o: int): I;
	pack:	fn(i: self I, d: array of byte);
};

I.unpack(d: array of byte, o: int): I
{
	i: I;
	i.t = int d[o++];
	a := array[20] of byte;
	a[:] = d[o:o+20];
	i.s.a = a;
	o += 20;
	i.n = int d[o+0]<<8|int d[o+1]<<0;
	return i;
}

I.pack(i: self I, d: array of byte)
{
	o := 0;
	d[o++] = byte i.t;
	d[o:] = i.s.a;
	o += 20;
	d[o+0] = byte (i.n>>8);
	d[o+1] = byte (i.n>>0);
	o += 2;
}

Chain: adt {
	c:	array of I;
	n:	int;
	next:	ref Chain;
};
heads: array of ref Chain;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	kr = load Keyring Keyring->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	sys->pctl(sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-dtV] [-c] [-a addr] [-i index] [-f data]");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'a' =>	addr = arg->earg();
		'c' =>	cflag++;
		'i' =>	index = arg->earg();
		'f' =>	data = arg->earg();
		't' =>	tflag++;
		'V' =>	Vflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 0)
		arg->usage();

	addr = dial->netmkaddr(addr, "tcp", "venti");
	ac := dial->announce(addr);
	if(ac == nil)
		fail(sprint("announce %q: %r", addr));

	ifd = sys->open(index, sys->ORDWR);
	dfd = sys->open(data, sys->ORDWR);
	if(ifd == nil || dfd == nil) {
		if(!cflag)
			fail(sprint("open: %r (try -c)"));
		if(ifd != nil || dfd != nil)
			fail("need both index and data file or neither");
		ifd = sys->create(index, sys->ORDWR, 8r666);
		dfd = sys->create(data, sys->ORDWR, 8r666);
		if(ifd == nil || dfd == nil)
			fail(sprint("create: %r"));
	}
	heads = array[2**12] of ref Chain;
	read();

	lookupc = chan[1] of (Score, int, R, chan of R);
	putc = chan[1] of (Score, int, array of byte, R, chan of R);
	syncc = chan[1] of (R, chan of R);

	spawn listen(ac);
	spawn serve();
}

listen(ac: ref dial->Connection)
{
	for(;;) {
		lc := dial->listen(ac);
		if(lc == nil)
			return warn(sprint("listen: %r"));
		spawn pproto(lc);
		lc = nil;
	}
}

W: adt {
	s:	Score;
	t:	int;
	d:	array of byte;
	r:	list of ref (R, chan of R);	# writers & syncers

	o:	big;
};

serve()
{
	writec := chan[1] of list of ref W;
	writtenc := chan[1] of string;
	spawn writer(writec, writtenc);

	writes: list of ref W;  # in progress
	nwrites: list of ref W;  # next batch
	allwrites: list of ref W;  # both writes & nwrites

	ntokens := Ntokens;
	pc := putc;
	sc := syncc;
	nopc := chan of (Score, int, array of byte, R, chan of R);
	nosc := chan of (R, chan of R);

loop:
	for(;;) {
		nw: ref W;
		alt {
		(s, t, r, rc) := <-lookupc =>
			(ok, i) := lookup(t, s);
			if(ok) {
				r.n = i.n;
				r.o = i.o;
			} else
				r.err = "no such score";
			rc <-= r;
			continue;

		(s, t, d, r, rc) := <-pc =>
			for(l := allwrites; l != nil; l = tl l) {
				w := hd l;
				if(w.t == t && w.s.eq(s)) {
					w.r = ref (r, rc)::w.r;
					continue loop;
				}
			}
			(ok, nil) := lookup(t, s);
			if(ok) {
				rc <-= r;
				continue;
			}
			nw = ref W(s, t, d, ref (r, rc)::nil, big -1);

		(r, rc) := <-sc =>
			nw = ref W(Score(nil), -1, nil, ref (r, rc)::nil, big -1);

		err := <-writtenc =>
			for(wl := writes; wl != nil; wl = tl wl) {
				w := hd wl;
				for(l := w.r; l != nil; l = tl l) {
					(r, rc) := *hd l;
					r.err = err;
					rc <-= r;
				}
				if(w.t >= 0)
					add(I(w.t, w.s, len w.d, w.o));
				ntokens++;
			}
			writes = nil;
			allwrites = nwrites;
		}

		if(nw != nil) {
			allwrites = nw::allwrites;
			nwrites = nw::nwrites;
			ntokens--;
			nw = nil;
		}
		if(writes == nil && nwrites != nil) {
			writec <-= nwrites;
			writes = nwrites;
			nwrites = nil;
		}
		if(ntokens > 0) {
			pc = putc;
			sc = syncc;
		} else {
			pc = nopc;
			sc = nosc;
		}
	}
}

writer(wc: chan of list of ref W, rc: chan of string)
{
	werr: string;
	for(;;) {
		wl := <-wc;
		if(werr != nil) {
			rc <-= werr;
			continue;
		}

		nd := 0;
		for(l := wl; l != nil; l = tl l)
			nd += len (hd l).d;
		id := array[len wl*Isize] of byte;
		dd := array[len wl*Isize+nd] of byte;

		sync := 0;
		oi := 0;
		od := 0;
		for(; wl != nil; wl = tl wl) {
			w := hd wl;
			if(w.t < 0) {
				sync++;
				continue;
			}
			w.o = doff+big od;
			i := I(w.t, w.s, len w.d, w.o);
			i.pack(id[oi:]);
			dd[od:] = id[oi:oi+Isize];
			dd[od+Isize:] = w.d;
			oi += Isize;
			od += Isize+len w.d;
		}
		if(oi > 0) {
			if(sys->write(ifd, id, len id) == len id && sys->pwrite(dfd, dd, len dd, doff) == len dd) {
				ioff += big oi;
				doff += big od;
			} else
				werr = sprint("write: %r");
		}
		if(sync)
		if(werr == nil)
		if(sys->fwstat(ifd, sys->nulldir) != 0 || sys->fwstat(dfd, sys->nulldir) != 0)
			werr = sprint("wstat: %r");
		rc <-= werr;
	}
}

rl(fd: ref Sys->FD): string
{
	d := array[128] of byte;
	o := 0;
	for(;;)
	case sys->read(fd, d[o:], 1) {
	0 =>
		sys->werrstr("eof");
		return nil;
	1 =>
		if(d[o++] == byte '\n')
			return string d[:o];
		if(o == len d) {
			sys->werrstr("long handshake");
			return nil;
		}
	* =>
		return nil;
	}
}

pproto(lc: ref Sys->Connection)
{
	sys->pctl(sys->NEWPGRP, nil);
	proto(lc);
	killgrp(pid());
}

proto(lc: ref Sys->Connection)
{
	fd := dial->accept(lc);
	if(fd == nil)
		return warn(sprint("accept: %r"));

	if(sys->fprint(fd, "venti-02-tventi\n") < 0)
		return say(sprint("write handshake: %r"));
	hs := rl(fd);
	if(hs == nil)
		return say(sprint("read handshake: %r"));
	say("client handshake: "+hs[:len hs-1]);

	(hh, herr) := Vmsg.read(fd);
	if(herr != nil)
		return say("read Thello: "+herr);
	if(tflag) warn("<- "+hh.text());
	pick h := hh {
	Thello =>
		if(h.version != "02")
			return say(sprint("client sent invalid venti version %q", h.version));
		rm := ref Vmsg.Rhello(0, hh.tid, nil, 0, 0);
		if(tflag) warn("-> "+rm.text());
		if(sys->write(fd, d := array of byte rm.pack(), len d) != len d)
			return say(sprint("write Rhello: %r"));
	* =>
		return say("client did not set Thello");
	}

	tmc := chan[1] of ref Vmsg;
	rmc := chan[1] of ref Vmsg;
	errc := chan of string;
	rc := chan[Ntokens] of R;

	spawn vreader(fd, tmc, errc);
	spawn vwriter(fd, rmc, errc);

	for(;;)
	alt {
	e := <-errc =>
		return say("error: "+e);

	mm := <-tmc =>
		r := zeror;
		r.m = mm;
		pick m := mm {
		Tping =>
			rmc <-= ref Vmsg.Rping(0, mm.tid);
		Tread =>
			lookupc <-= (m.score, m.etype, r, rc);
		Twrite =>
			if(len m.data > venti->Maxlumpsize) {
				rmc <-= ref Vmsg.Rerror(0, mm.tid, "too big");
			} else {
				r.s = Score(sha1(m.data));
				putc <-= (r.s, m.etype, m.data, r, rc);
			}
		Tsync =>
			syncc <-= (r, rc);
		Tgoodbye =>
			return say("client quit");
		* =>
			return say("unexpected vmsg");
		}

	r := <-rc =>
		tid := r.m.tid;
		err := r.err;
		rm: ref Vmsg;
		if(err == nil)
		pick m := r.m {
		Tread =>
			if(m.n == 0)
				rm = ref Vmsg.Rread(0, 0, array[0] of byte);
			else if(m.n != 0 && m.n < r.n)
				err = "block too big";
			else if(preadn(dfd, d := array[r.n] of byte, len d, r.o+big Isize) != len d)
				err = sprint("read: %r");
			else if(Vflag && !m.score.eq(rs := Score(sha1(d))))
				err = sprint("bad data from disk, expected %s, saw %s", m.score.text(), rs.text());
			else
				rm = ref Vmsg.Rread(0, tid, d);
		Twrite =>
			rm = ref Vmsg.Rwrite(0, tid, r.s);
		Tsync => 
			rm = ref Vmsg.Rsync(0, tid);
		}
		if(err != nil)
			rmc <-= ref Vmsg.Rerror(0, tid, err);
		else
			rmc <-= rm;
	}
}

vreader(fd: ref Sys->FD, c: chan of ref Vmsg, errc: chan of string)
{
	for(;;) {
		(m, err) := Vmsg.read(fd);
		if(err != nil) {
			errc <-= err;
			return;
		}
		if(tflag) warn("<- "+m.text());
		c <-= m;
	}
}

vwriter(fd: ref Sys->FD, c: chan of ref Vmsg, errc: chan of string)
{
	for(;;) {
		m := <-c;
		if(tflag) warn("-> "+m.text());
		if(sys->write(fd, d := m.pack(), len d) != len d)
			errc <-= sprint("write: %r");
	}
}

read()
{
	(iok, id) := sys->fstat(ifd);
	(dok, dd) := sys->fstat(dfd);
	if(iok != 0 || dok != 0)
		fail(sprint("fstat: %r"));
	if(id.length % big Isize != big 0)
		fail(sprint("index length %bd not multiple of Isize %d", id.length, Isize));
	n := int id.length;
	buf := array[n] of byte;
	if(sys->readn(ifd, buf, len buf) != len buf)
		fail(sprint("read index: %r"));
	for(o := 0; o < n; o += Isize) {
		i := I.unpack(buf, o);
		i.o = doff;
		doff += big (Isize+i.n);
		add(i);
	}
	ioff = id.length;
	if(doff != dd.length)
		fail(sprint("doff at %bd, expected at %bd", doff, dd.length));
}

head(s: Score): int
{
	x := int s.a[0]<<8|int s.a[1]<<0;
	x &= (len heads-1);
	return x;
}

add(i: I)
{
	x := head(i.s);
	c := heads[x];
	if(c == nil || c.n == len c.c)
		heads[x] = c = ref Chain(array[8] of I, 0, c);
	c.c[c.n++] = i;
}

zeroi: I;
lookup(t: int, s: Score): (int, I)
{
	x := head(s);
	for(c := heads[x]; c != nil; c = c.next)
		for(i := c.n-1; i >= 0; i--)
			if(c.c[i].t == t && c.c[i].s.eq(s))
				return (1, c.c[i]);
	return (0, zeroi);
}

preadn(fd: ref Sys->FD, d: array of byte, n: int, o: big): int
{
	h := 0;
	while(h < n) {
		nn := sys->pread(fd, d[h:], n-h, o+big h);
		if(nn < 0)
			return nn;
		if(nn == 0)
			break;
		h += nn;
	}
	return h;
}

pid(): int
{
	return sys->pctl(0, nil);
}

progctl(pid: int, s: string)
{
	sys->fprint(sys->open(sprint("/prog/%d/ctl", pid), sys->OWRITE), "%s", s);
}

killgrp(pid: int)
{
	progctl(pid, "killgrp");
}

sha1(d: array of byte): array of byte
{
	kr->sha1(d, len d, dig := array[kr->SHA1dlen] of byte, nil);
	return dig;
}

warn(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
}

say(s: string)
{
	if(dflag)
		warn(s);
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}
