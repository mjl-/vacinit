# for each incoming connection we read venti read requests which
# we forward to a local venti server.
# if the local server does not have the block, we ask the remote server.
# if the remote server has it, we write the data to the local server
# (on a different connection than we request reads on), but first
# we reply to the client.
# each incoming client is served independently from others, so each
# client has its own connections to the remote and local server.
# for each client, we allow at most 256 incoming requests in progress,
# using a simple token mechanism.  this ensures we never have more
# than 256 requests pending to the remote or localwrite servers too.

implement Xventi;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "dial.m";
	dial: Dial;
include "tables.m";
	tables: Tables;
	Table: import tables;
include "keyring.m";
	kr: Keyring;
include "venti.m";
	venti: Venti;
	Session, Score, Vmsg: import venti;

Xventi: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

dflag: int;
tflag: int;
vflag: int;
laddr := "*";
remaddr: string;
localaddr: string;

xload[T](m: T): T
{
	if(m == nil)
		fail(sprint("load: %r"));
	return m;
}

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := xload(load Arg Arg->PATH);
	dial = xload(load Dial Dial->PATH);
	tables = xload(load Tables Tables->PATH);
	kr = xload(load Keyring Keyring->PATH);
	venti = xload(load Venti Venti->PATH);
	venti->init();

	sys->pctl(sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-dtv] [-a laddr] remoteaddr localaddr");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'a' =>	laddr = arg->earg();
		't' =>	tflag++;
		'v' =>	vflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 2)
		arg->usage();
	laddr = dial->netmkaddr(laddr, "tcp", "venti");
	remaddr = dial->netmkaddr(hd args, "net", "venti");
	localaddr = dial->netmkaddr(hd tl args, "net", "venti");
	say(sprint("remaddr %q, localaddr %q", remaddr, localaddr));

	ac := dial->announce(laddr);
	if(ac == nil)
		fail(sprint("announce: %r"));
	spawn listen(ac);
}

listen(ac: ref dial->Connection)
{
	for(;;) {
		lc := dial->listen(ac);
		if(lc == nil)
			return warn(sprint("listen: %r"));
		spawn proto(lc);
		lc = nil;
	}
}

proto(c: ref dial->Connection)
{
	fd := dial->accept(c);
	if(fd == nil)
		return say(sprint("accept: %r"));

	if(sys->fprint(fd, "venti-02-xventi\n") < 0)
		return say(sprint("write handshake: %r"));
	s := rl(fd);
	if(s == nil)
		return say(sprint("read handshake: %r"));

	(th, err) := Vmsg.read(fd);
	if(err != nil)
		return say("read thello: "+err);
	if(tflag) warn("client <- "+th.text());
	lfd: ref Sys->FD;
	pick t := th {
	Thello =>
		if(t.version != "02")
			return say(sprint("client picked bad version %#q", t.version));

		r: ref Vmsg;
		r = ref Vmsg.Rhello(0, t.tid, nil, 0, 0);

		say(sprint("dial localaddr %q for first lookups", localaddr));
		lfd = vdial(localaddr);
		if(lfd == nil) {
			err = sprint("dial local: %r");
			r = ref Vmsg.Rerror(0, t.tid, err);
		}

		if(tflag) warn("client -> "+r.text());
		if(sys->write(fd, d := r.pack(), len d) != len d)
			return say(sprint("write rhello: %r"));
		if(err != nil)
			return say(err);
	* =>
		return say("first tmsg not hello");
	}

	sys->pctl(sys->NEWPGRP, nil);
	protox(fd, lfd);
	killgrp(pid());
}

creader(fd: ref Sys->FD, tc: chan of byte, c: chan of ref Vmsg)
{
	for(;;) {
		<-tc;
		(m, err) := Vmsg.read(fd);
		if(err != nil)
			fail(sprint("client: read: %r"));
		if(tflag) warn("client <- "+m.text());
		c <-= m;
	}
}

cwriter(fd: ref Sys->FD, tc: chan of byte, c: chan of (ref Vmsg, int))
{
	for(;;) {
		(m, t) := <-c;
		if(tflag) warn("client -> "+m.text());
		if(sys->write(fd, d := m.pack(), len d) != len d)
			fail(sprint("client: write: %r"));
		if(t)
			tc <-= byte 0;
	}
}

writer(fd: ref Sys->FD, c: chan of ref Vmsg, ec: chan of string, who: string, pidc: chan of int)
{
	if(pidc != nil)
		pidc <-= pid();
	for(;;) {
		m := <-c;
		if(tflag) warn(who+" -> "+m.text());
		if(sys->write(fd, d := m.pack(), len d) != len d)
			ec <-= sprint("write: %r");
	}
}

reader(fd: ref Sys->FD, c: chan of ref Vmsg, ec: chan of string, who: string, pidc: chan of int)
{
	if(pidc != nil)
		pidc <-= pid();
	for(;;) {
		(m, err) := Vmsg.read(fd);
		if(tflag) warn(who+" <- "+m.text());
		if(err != nil) {
			ec <-= sprint("read: %r");
			return;
		}
		c <-= m;
	}
}

# first char in chan name is src/dst of message.
# second char is type of message t for request, r for response.
# chans to writers are buffered so we never block.
# to ratelimit clients, the client msg reader needs a token to read a message.
# tokens are returned when all traffic (to remote, localwrite) has been completed.
# we forward read requests from clients unmodified to the local and possibly remote server.
# for the localwrite server we need different tids since the client
# may reuse the tid after we sent the response when the Rread from remote came in.
# we still have the 256 token limit though, so a tid is always available.
protox(cfd, lfd: ref Sys->FD)
{
say("protox");
	tokenc := chan[256] of byte;
	for(i := 0; i < 256; i++)
		tokenc <-= byte 0;

	ctc := chan[1] of ref Vmsg;
	crc := chan[256] of (ref Vmsg, int);
	spawn creader(cfd, tokenc, ctc);
	spawn cwriter(cfd, tokenc, crc);

	ltc := chan[256] of ref Vmsg;
	lrc := chan[1] of ref Vmsg;
	lerrc := chan of string;
	spawn writer(lfd, ltc, lerrc, "local", nil);
	spawn reader(lfd, lrc, lerrc, "local", nil);

	ctidtab := Table[ref Vmsg.Tread].new(31, nil);

	rfd: ref Sys->FD;
	rerrc := chan of string;
	rpids: list of int;
	# replaced with buffered channels before spawning reader & writer
	rtc := chan of ref Vmsg;
	rrc := chan of ref Vmsg;

	wfd: ref Sys->FD;
	wtc := chan[256] of ref Vmsg;
	wrc := chan[1] of ref Vmsg;
	werrc := chan of string;
	wtids: list of int;
	wtidtab: ref Table[ref Vmsg.Tread];

	for(;;)
	alt {
	mm := <-ctc =>
		tid := mm.tid;
		pick m := mm {
		Tread =>
			if(ctidtab.find(tid) != nil)
				fail("client sent duplicate tid");
			m.istmsg = 1;  # underway to local
			ltc <-= m;
			ctidtab.add(tid, m);
		Twrite =>
			crc <-= (ref Vmsg.Rerror(0, tid, "read-only"), 1);
		Tsync =>
			crc <-= (ref Vmsg.Rerror(0, tid, "read-only"), 1);
		Tping =>
			crc <-= (ref Vmsg.Rping(0, tid), 1);
		Tgoodbye =>
			fail("client quit");
		* =>
			fail("unexpected message from client");
		}

	e := <-lerrc =>
		fail("i/o error to/from local server: "+e);

	mm := <-lrc =>
		tid := mm.tid;
		tm := ctidtab.find(tid);
		if(tm == nil)
			fail(sprint("local server sent bogus tid %d", tid));
		pick m := mm {
		Rerror =>
			# we assume error is that local server does not have it
			if(rfd == nil) {
				say(sprint("dial remaddr %q for authoritative lookups", remaddr));
				rfd = vdial(remaddr);
				if(rfd == nil) {
					err := sprint("dial remote %q: %r", remaddr);
					warn(err);
					ctidtab.del(tid);
					crc <-= (ref Vmsg.Rerror(0, tid, err), 1);
					continue;
				}
				rtc = chan[256] of ref Vmsg;
				rrc = chan[1] of ref Vmsg;
				pidc := chan of int;
				spawn writer(rfd, rtc, rerrc, "remote", pidc);
				spawn reader(rfd, rrc, rerrc, "remote", pidc);
				rpids = list of {<-pidc, <-pidc};
			}
			tm.istmsg = 2;  # underway to remote
			rtc <-= tm;
		Rread =>
			ctidtab.del(tid);
			if(vflag && !tm.score.eq(ns := Score(sha1(m.data)))) {
				err := sprint("local server returned bad data, score %s instead of %s", tm.score.text(), ns.text());
				warn(err);
				crc <-= (ref Vmsg.Rerror(0, tid, err), 1);
			} else
				crc <-= (mm, 1);
		* =>
			fail("local server sent bogus message");
		}

	e := <-rerrc =>
		warn("i/o error to/from remote: "+e);
		kill(hd rpids);
		kill(hd tl rpids);
		rpids = nil;
		rtc = chan of ref Vmsg;
		rrc = chan of ref Vmsg;
		rfd = nil;

		otids: list of int;
		for(i = 0; i < len ctidtab.items; i++) {
			for(l := ctidtab.items[i]; l != nil; l = tl l)
				if((hd l).t1.istmsg == 2)  # was underway to remote
					otids = (hd l).t1.tid::otids;
		}

		if(otids == nil)
			continue;

		say(sprint("redial remaddr %q for authoritative lookups", remaddr));
		rfd = vdial(remaddr);
		if(rfd == nil) {
			warn(err := sprint("redial remote %q: %r", remaddr));
			for(l := otids; l != nil; l = tl l) {
				ctidtab.del(hd l);
				crc <-= (ref Vmsg.Rerror(0, hd l, err), 1);
			}
		} else {
			rtc = chan[256] of ref Vmsg;
			rrc = chan[1] of ref Vmsg;
			pidc := chan of int;
			spawn writer(rfd, rtc, rerrc, "remote", pidc);
			spawn reader(rfd, rrc, rerrc, "remote", pidc);
			rpids = list of {<-pidc, <-pidc};

			for(l := otids; l != nil; l = tl l)
				rtc <-= ctidtab.find(hd l);
		}

	mm := <-rrc =>
		tid := mm.tid;
		tm := ctidtab.find(tid);
		if(tm == nil)
			fail(sprint("remote server sent bogus tid %d", tid));
		ctidtab.del(tid);
		pick m := mm {
		Rerror =>
			crc <-= (m, 1);
		Rread =>
			if(vflag && !tm.score.eq(ns := Score(sha1(m.data)))) {
				err := sprint("remote server returned bad data, score %s instead of %s", ns.text(), tm.score.text());
				warn(err);
				crc <-= (ref Vmsg.Rerror(0, tid, err), 1);
				continue;
			}
			crc <-= (m, 0);
			if(wfd == nil) {
				say(sprint("dial localaddr %q for writes", localaddr));
				wfd = vdial(localaddr);
				if(wfd == nil) {
					warn(sprint("dial localwrite %q: %r", localaddr));
					tokenc <-= byte 0;
					continue;
				}
				wtids = mktids();
				wtidtab = wtidtab.new(31, nil);
				spawn writer(wfd, wtc, werrc, "localwrite", nil);
				spawn reader(wfd, wrc, werrc, "localwrite", nil);
			}
			tid = hd wtids;
			wtids = tl wtids;
			wtc <-= ref Vmsg.Twrite(1, tid, tm.etype, m.data);
			tm.tid = tid;
			wtidtab.add(tid, tm);
		* =>
			fail("remote server sent bogus message");
		}

	e := <-werrc =>
		fail("i/o error to/from local write server: "+e);

	mm := <-wrc =>
		tid := mm.tid;
		tm := wtidtab.find(tid);
		if(tm == nil)
			fail(sprint("local write server sent bogus tid %d", tid));
		wtidtab.del(tid);
		wtids = tid::wtids;
		pick m := mm {
		Rerror =>
			warn("error from local write server: "+m.e);
		Rwrite =>
			if(!tm.score.eq(m.score))
				warn(sprint("local wrote %s, expected %s", m.score.text(), tm.score.text()));
		* =>
			fail("local write server sent bogus message");
		}
		tokenc <-= byte 0;
	}
}

mktids(): list of int
{
	l: list of int;
	for(i := 0; i < 256; i++)
		l = i::l;
	return l;
}

vdial(addr: string): ref Sys->FD
{
	say("dial "+addr);
	c := dial->dial(addr, nil);
	if(c == nil || Session.new(c.dfd) == nil)
		return nil;
	return c.dfd;
}

rl(fd: ref Sys->FD): string
{
	d := array[128] of byte;
	for(i := 0; i < len d; i++)
		case sys->read(fd, d[i:], 1) {
		0 =>
			sys->werrstr("eof");
			return nil;
		1 =>
			if(d[i] == byte '\n')
				return string d[:i];
		* =>
			return nil;
		}
	sys->werrstr("long line");
	return nil;
}

sha1(d: array of byte): array of byte
{
	dig := array[kr->SHA1dlen] of byte;
	kr->sha1(d, len d, dig, nil);
	return dig;
}

progctl(pid: int, s: string)
{
	sys->fprint(sys->open(sprint("/prog/%d/ctl", pid), sys->OWRITE), "%s", s);
}

kill(pid: int)
{
	progctl(pid, "kill");
}

killgrp(pid: int)
{
	progctl(pid, "killgrp");
}

pid(): int
{
	return sys->pctl(0, nil);
}

fd2: ref Sys->FD;
warn(s: string)
{
	if(fd2 == nil)
		fd2 = sys->fildes(2);
	sys->fprint(fd2, "%s\n", s);
}

say(s: string)
{
	if(dflag)
		warn(s);
}

fail(s: string)
{
	warn(s);
	killgrp(pid());
	raise "fail:"+s;
}
