implement Slowventi;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "dial.m";
	dial: Dial;
include "string.m";
	str: String;
include "venti.m";
	venti: Venti;
	Vmsg, Session: import venti;

Slowventi: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

uprate := -1;
downrate := -1;
latency := -1;
addr := "*";
remaddr: string;
tflag: int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	sys->pctl(Sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-t] [-a addr] [-u rate] [-d rate] [-r rtt] addr");
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	addr = arg->earg();
		'd' =>	downrate = rate(arg->earg());
		'u' =>	uprate = rate(arg->earg());
		'r' =>	latency = int arg->earg()/2;
		't' =>	tflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 1)
		arg->usage();
	remaddr = dial->netmkaddr(hd args, "net", "venti");

	addr = dial->netmkaddr(addr, "tcp", "venti");
	ac := dial->announce(addr);
	if(ac == nil)
		fail(sprint("announce %q: %r", addr));

	spawn listen(ac);
}

rate(s: string): int
{
	(v, rem) := str->toint(s, 10);
	case str->tolower(rem) {
	"" =>	{}
	"k" =>	v *= 1024**1;
	"m" =>	v *= 1024**2;
	"g" =>	v *= 1024**3;
	* =>
		fail("bad rate");
	}
	return v;
}

listen(ac: ref dial->Connection)
{
	for(;;) {
		lc := dial->listen(ac);
		if(lc == nil)
			return warn(sprint("listen: %r"));
		spawn proto(lc);
	}
}

proto(c: ref dial->Connection)
{
	sys->pctl(Sys->NEWPGRP, nil);

	cfd := dial->accept(c);
	if(cfd == nil)
		return warn(sprint("accept: %r"));

	cc := dial->dial(remaddr, nil);
	if(cc == nil)
		return warn(sprint("dial %q: %r", remaddr));
	rfd := cc.dfd;

	ss := Session.new(rfd);
	if(ss == nil)
		return warn(sprint("handshake: %r"));


	if(sys->fprint(cfd, "venti-02-slowventi\n") < 0)
		return warn(sprint("write version: %r"));
	if(rl(cfd) == nil)
		return warn(sprint("read version: %r"));
	(mm, err) := Vmsg.read(cfd);
	if(err != nil)
		return warn(sprint("reading thello: %r"));
	pick m := mm {
	Thello =>
		if(m.version != "02")
			return warn(sprint("client chose unsupported version %#q", m.version));
		rm := ref Vmsg.Rhello(0, m.tid, "", 0, 0);
		if(sys->write(cfd, d := rm.pack(), len d) != len d)
			return warn(sprint("writing rhello: %r"));
	* =>
		return warn("first message from client not thello");
	}


	tokenc := chan[256] of byte;
	for(i := 0; i < 256; i++)
		tokenc <-= byte 0;
	tc := chan[256] of ref Vmsg;
	rc := chan[256] of ref Vmsg;
	spawn lreader(cfd, tc, tokenc);
	spawn rwriter(rfd, tc);
	spawn rreader(rfd, rc);
	spawn lwriter(cfd, rc, tokenc);
}

rl(fd: ref Sys->FD): string
{
	d := array[128] of byte;
	o := 0;
	for(;;)
	case sys->read(fd, d[o:], 1) {
	0 =>
		sys->werrstr("early eof");
		return nil;
	1 =>
		if(d[o++] == byte '\n')
			return string d[:o];
		if(o >= len d) {
			sys->werrstr("long line");
			return nil;
		}
	* =>
		return nil;
	}
}

lreader(fd: ref Sys->FD, c: chan of ref Vmsg, tc: chan of byte)
{
	for(;;) {
		<-tc;
		(m, err) := Vmsg.read(fd);
		if(err != nil)
			fail("client read: "+err);
		if(tflag) warn("client <- "+m.text());
		if(latency > 0)
			spawn delaysend(c, m);
		else
			c <-= m;
	}
}

delaysend(c: chan of ref Vmsg, m: ref Vmsg)
{
	sys->sleep(latency);
	c <-= m;
}

rwriter(fd: ref Sys->FD, c: chan of ref Vmsg)
{
	last := sys->millisec();
	for(;;) {
		m := <-c;
		if(sys->write(fd, d := m.pack(), len d) != len d)
			fail(sprint("remote write: %r"));
		if(uprate > 0) {
			now := sys->millisec();
			ms := 1000*len d/uprate;
			ms -= max(0, min(1000, now-last));
			if(ms > 0)
				sys->sleep(ms);
			last = now+ms;
		}
		if(tflag) warn("remote -> "+m.text());
	}
}

rreader(fd: ref Sys->FD, c: chan of ref Vmsg)
{
	last := sys->millisec();
	for(;;) {
		(m, err) := Vmsg.read(fd);
		if(err != nil)
			fail("remote read: "+err);
		if(downrate > 0) {
			now := sys->millisec();
			ms := 1000*len m.pack()/downrate;
			ms -= max(0, min(1000, now-last));
			if(ms > 0)
				sys->sleep(ms);
			last = now+ms;
		}
		if(tflag) warn("remote <- "+m.text());
		if(latency > 0)
			spawn delaysend(c, m);
		else
			c <-= m;
	}
}

lwriter(fd: ref Sys->FD, c: chan of ref Vmsg, tc: chan of byte)
{
	for(;;) {
		m := <-c;
		if(tflag) warn("client -> "+m.text());
		if(sys->write(fd, d := m.pack(), len d) != len d)
			fail(sprint("client write: %r"));
		tc <-= byte 0;
	}
}

min(a, b: int): int
{
	if(a < b)
		return a;
	return b;
}

max(a, b: int): int
{
	if(a > b)
		return a;
	return b;
}

progctl(pid: int, s: string)
{
	sys->fprint(sys->open(sprint("/prog/%d/ctl", pid), sys->OWRITE), "%s", s);
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

fail(s: string)
{
	warn(s);
	killgrp(pid());
	raise "fail:"+s;
}
