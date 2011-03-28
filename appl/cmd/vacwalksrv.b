implement Vacwalksrv;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "dial.m";
	dial: Dial;
include "string.m";
	str: String;
include "venti.m";
	venti: Venti;
	Score: import venti;

Vacwalksrv: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};
Cmd: type Vacwalksrv;

addr := "*";
vaddr: string;
reapc: chan of int;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	bufio = load Bufio Bufio->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	venti = load Venti Venti->PATH;

	sys->pctl(Sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-l addr] ventiaddr");
	while((c := arg->opt()) != 0)
		case c {
		'l' =>	addr = arg->earg();
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 1)
		arg->usage();
	vaddr = hd args;

	addr = dial->netmkaddr(addr, "tcp", "5657");
	ac := dial->announce(addr);
	if(ac == nil)
		fail(sprint("announce %q: %r", addr));

	reapc = chan of int;
	spawn reaper();
	spawn listen(ac);
}

reaper()
{
	for(;;)
		killgrp(<-reapc);
}

listen(ac: ref dial->Connection)
{
	for(;;) {
		lc := dial->listen(ac);
		if(lc == nil)
			return warn(sprint("listen: %r"));
		spawn srv(lc);
		lc = nil;
	}
}

srv(c: ref dial->Connection)
{
	rfd := dial->accept(c);
	if(rfd == nil)
		return warn(sprint("accept: %r"));

	m := load Cmd "/dis/vacsrv.dis";
	if(m == nil)
		return warn(sprint("load vacsrv: %r"));

	buf := array[2*venti->Scoresize+1] of byte;
	if(sys->readn(rfd, buf, len buf) != len buf)
		return warn(sprint("read vac score: %r"));
	if(buf[len buf-1] != byte '\n')
		return warn("bad line with score");
	score := string buf[:len buf-1];
	if(!scoreok(score))
		return warn("bad score");

	{
		sys->pctl(sys->NEWPGRP|sys->NEWFD|Sys->FORKNS, list of {rfd.fd, 2});
		sys->dup(rfd.fd, 1);
		m->init(nil, list of {"vacsrv", "-pV", "-a", vaddr, "-m", "/", score});
		sys->pctl(sys->NODEVS, nil);
		sys->chdir("/");

		fds: list of ref Sys->FD;  # hang on to fds of dirs, to keep blocks cached in vacsrv
		dat := array[8*1024] of byte;
		b := bufio->fopen(rfd, bufio->OREAD);
		for(;;) {
			s := b.gets('\n');
			if(s == nil || s == "\n")
				break;
			if(str->prefix("#", s))
				continue;
			if(s[len s-1] == '\n')
				s = s[:len s-1];
			fd := sys->open(s, sys->OREAD);
			if(fd == nil)
				continue;
			(ok, dir) := sys->fstat(fd);
			if(ok == 0 && (dir.mode&Sys->DMDIR))
				fds = fd::fds;
			while(sys->read(fd, dat, len dat) > 0)
				{}
			if(len fds >= 64)
				fds = nil;
		}
	} exception ex {
	"fail:*" =>
		warn(ex);
	"*" =>
		warn("broken: "+ex);
	}
	reapc <-= pid();
	# keep pid alive so we can be killgrp'ed
	hold := chan of int;
	<-hold;
}

scoreok(s: string): int
{
	if(len s != 40)
		return 0;
	for(i := 0; i < 40; i++)
	case s[i] {
	'0' to '9' or
	'a' to 'f' or
	'A' to 'F' =>	{}
	* =>		return 0;
	}
	return 1;
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

warn(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
}

fail(s: string)
{
	warn(s);
	killgrp(pid());
	raise "fail:"+s;
}
