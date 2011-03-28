implement Vacinit;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "env.m";
	env: Env;
include "draw.m";
include "arg.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "dial.m";
	dial: Dial;
include "keyring.m";
	kr: Keyring;
include "string.m";
	str: String;
include "sh.m";
	sh: Sh;
include "tk.m";
	tk: Tk;
include "tkclient.m";
	tkclient: Tkclient;
include "tables.m";
	tables: Tables;
	Strhash: import tables;
include "venti.m";
	venti: Venti;
	Score, Scoresize, Session, Vmsg: import venti;

Vacinit: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};
Cmd: type Vacinit;

dflag: int;
Vflag: int;

wdir: string;
names: array of string;
top: ref Tk->Toplevel;

Url: adt {
	proto:	string;
	addr:	string;
	path:	string;

	parse:	fn(s: string): (ref Url, string);
	text:	fn(u: self ref Url): string;
};

Cfg: adt {
	name:	string;
	score:	string;
	venti:	string;
	walk:	string;
	url:	string;
	run:	string;
	env:	list of ref (string, string);  # does not hold the other fields in this adt

	text:	fn(c: self ref Cfg): string;
};

init(ctxt: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	if(sys->bind("/vacinit", "/", sys->MAFTER) < 0)
		fail(sprint("bind /vacinit on /: %r"));
	arg := load Arg Arg->PATH;
	bufio = load Bufio Bufio->PATH;
	dial = load Dial Dial->PATH;
	kr = load Keyring Keyring->PATH;
	str = load String String->PATH;
	env = load Env Env->PATH;
	tk = load Tk Tk->PATH;
	tkclient = load Tkclient Tkclient->PATH;
	tkclient->init();
	tables = load Tables Tables->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	sys->pctl(sys->NEWPGRP, nil);

	dflag++;

	arg->init(args);
	arg->setusage(arg->progname()+" [-dV]");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'V' =>	Vflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 0)
		arg->usage();

	xbind("#e", "/env", sys->MREPL|sys->MCREATE);
	emuwdir := env->getenv("emuwdir");
say(sprint("emuwdir %q", emuwdir));
	wdir = "#U"+emuwdir;
	if(sys->stat(wdir).t0 != 0) {
		wdir = "#U*"+emuwdir;
		if(sys->stat(wdir).t0 != 0)
			fail("cannot stat emuwdir");
	}
say(sprint("wdir %q", wdir));

	# ensure lib/ and lib/vacinit/ exist
	initmsg: string;
	if(sys->stat(wdir+"/lib").t0 != 0) {
		if(sys->create(wdir+"/lib", sys->OREAD, sys->DMDIR|8r777) == nil)
			initmsg += sprint("mkdir lib: %r\n");
		else
			initmsg += "directory lib/ created\n";
	}
	if(sys->stat(wdir+"/lib/vacinit").t0 != 0) {
		if(sys->create(wdir+"/lib/vacinit", sys->OREAD, sys->DMDIR|8r777) == nil)
			initmsg += sprint("mkdir lib/vacinit: %r\n");
		else
			initmsg += "directory lib/vacinit/ created\n";
	}

	sys->bind(wdir+"/lib/vacinit", "/vacinit/lib/vacinit", sys->MCREATE|sys->MBEFORE);

	cs();

	if(sys->stat("/vacinit/lib/vacinit/default").t0 == 0) {
		say("using default");
		(cc, err) := rc("default");
		if(err != nil)
			fail("reading default: "+err);
		spawn run(cc, pid());
		wc := chan of int;
		<-wc;
	}

	if(ctxt == nil)
		ctxt = tkclient->makedrawcontext();
	if(ctxt == nil)
		fail("no window context");

	(tktop, wmctl) := tkclient->toplevel(ctxt, "", "vacinit", Tkclient->Plain);
	top = tktop;

	cmdc := chan of string;
	tk->namechan(top, cmdc, "cmd");
	tkcmd("listbox .e -selectmode browse");
	tkcmd("bind .e {<Key-\t>}          {focus .n.url}");
	tkcmd("bind .e {<Key-\n>}          {send cmd run}");
	tkcmd("bind .e {<Button-1>}        +{send cmd select}");
	tkcmd("bind .e {<Double-Button-1>} +{send cmd run}");

	tkcmd("frame .n");
	tkcmd("label .n.new -text {fetch config by http or 9p url:}");
	tkcmd("entry .n.url");
	tkcmd("pack .n.new -anchor w");
	tkcmd("pack .n.url -fill x -expand 1");

	tkcmd("bind .n.url {<Key-\n>}		{send cmd fetch}");
	tkcmd("bind .n.url {<Key-\t>}		{focus .e}");
	tkcmd("bind .n.url <ButtonPress-2>	{send cmd paste %W}");

	tkcmd("frame .i");
	tkcmd("label .i.l -text {current config:}");
	tkcmd("text .i.txt -state disabled");
	tkcmd("pack .i.l .i.txt -fill x");

	tkcmd("frame .m");
	tkcmd("text .m.txt -state disabled");
	tkcmd("pack .m.txt -fill both -expand 1");

	tkcmd("pack .e -side left -fill y");
	tkcmd("pack .n .i -in .m -fill x -expand 0 -anchor w -padx 15 -pady 15");
	tkcmd("pack .m.txt -fill both -expand 1 -padx 10 -pady 10");
	tkcmd("pack .m -fill both -expand 1");

	nerr := setnames(nil);
	if(nerr != nil)
		warn(nerr);
	if(len names == 0)
		tkcmd("focus .n.url");
	tkmsg(initmsg);

	tkclient->onscreen(top, nil);
	tkclient->startinput(top, "kbd"::"ptr"::nil);

	for(;;)
	alt {
	s := <-top.ctxt.kbd =>
		tk->keyboard(top, s);

	s := <-top.ctxt.ptr =>
		tk->pointer(top, *s);

	s := <-top.ctxt.ctl or
	s = <-top.wreq or
	s = <-wmctl =>
		tkclient->wmctl(top, s);

	s := <-cmdc =>
		say("cmd: "+s);
		tkmsg("");
		case s {
		"fetch" =>
			cc: ref Cfg;
			url := tkcmd(".n.url get");
			(u, err) := Url.parse(url);
			if(err == nil)
				(cc, err) = fetch(u, "new");
			if(err == nil) {
				cc.name = str->splitstrr(u.path, "/").t1;
				cc.url = u.text();
				err = wc(cc);
			}
			if(err == nil)
				err = setnames(cc.name);
			if(err != nil)
				tkmsg(err);
		"select" or
		"run" =>
			i := int tkcmd(".e curselection");
			if(i < 0 || i >= len names) {
				tkmsg("no valid config selected");
				break;
			}
			(cc, err) := rc(names[i]);
			if(err != nil) {
				tkmsg(err);
				break;
			}
			case s {
			"select" =>
				tkcfg(cc);
			"run" =>
				spawn run(cc, pid());
				rc := chan of int;
				<-rc;
			}
		* =>
			if(str->prefix("paste ", s)) {
				w := s[len "paste ":];
				tkcmd(sprint("%s insert 0 '%s", w, tkclient->snarfget()));
			} else
				warn("unknown cmd: "+s);
		}
		tkcmd("update");
	}
}

Url.parse(s: string): (ref Url, string)
{
	if(str->prefix("http://", s))
		proto := "http";
	else if(str->prefix("9p://", s))
		proto = "9p";
	else
		return (nil, "unknown protocol");
	s = s[len proto+len "://":];
	(addr, path) := str->splitstrl(s, "/");
	if(path == nil || path[len path-1] == '/')
		return (nil, "empty path or path ends with /");
	path = path[1:];
	u := ref Url(proto, addr, path);
	return (u, nil);
}

Url.text(u: self ref Url): string
{
	return sprint("%s://%s/%s", u.proto, u.addr, u.path);
}


setnames(nm: string): string
{
	nms: list of string;
	dfd := sys->open("/vacinit/lib/vacinit", sys->OREAD);
	if(dfd == nil)
		return sprint("open: %r");
	tkcmd(".e delete 0 end");
	newest: ref sys->Dir;
	ni := -1;
	nmi := -1;
	j := 0;
	for(;;) {
		(n, dd) := sys->dirread(dfd);
		if(n < 0)
			return sprint("dirread: %r");
		if(n == 0)
			break;
		for(i := 0; i < len dd; i++) {
			case dd[i].name {
			"tdata" or
			"tindex" =>
				{}
			* =>
				if(newest == nil || dd[i].mtime > newest.mtime) {
					newest = ref dd[i];
					ni = j;
				}
				if(nm != nil && dd[i].name == nm)
					nmi = j;
				tkcmd(".e insert end '"+dd[i].name);
				nms = dd[i].name::nms;
				j++;
			}
		}
	}
	names = l2a(rev(nms));
	if(nm != nil && nmi < 0)
		err := sprint("missing name %#q", nm);
	if(nmi < 0) {
		nmi = ni;
		if(newest != nil)
			nm = newest.name;
	}
	if(nmi >= 0) {
		tkcmd(".e activate "+string nmi);
		c: ref Cfg;
		(c, err) = rc(nm);
		if(err == nil) {
			tkcfg(c);
			tkcmd("focus .e");
		}
	}
	return err;
}

tkcfg(c: ref Cfg)
{
	tkcmd(".n.url delete 0 end; .n.url insert 0 '"+c.url);
	tkcmd(".i.txt delete 1.0 end; .i.txt insert 1.0 '"+c.text());
}

nocrlf(s: string): string
{
	if(suffix("\r\n", s))
		return s[:len s-2];
	if(suffix("\n", s))
		return s[:len s-1];
	return s;
}

fetchhttp(u: ref Url, nm: string): (ref Cfg, string)
{
say(sprint("fetchhttp, url %q %q %q", u.proto, u.addr, u.path));
	naddr := dial->netmkaddr(u.addr, "net", "http");
	cc := dial->dial(naddr, nil);
	if(cc == nil)
		return (nil, sprint("dial: %r"));
	if(sys->fprint(cc.dfd, "GET /%s HTTP/1.0\r\nhost: %s\r\nconnection: close\r\n\r\n", u.path, u.addr) < 0)
		return (nil, sprint("write http request: %r"));

	b := bufio->fopen(cc.dfd, bufio->OREAD);
	s := b.gets('\n');
	if(s == nil)
		return (nil, sprint("read http response: %r"));

	# should be:  http/1.0 200 reason\r\n
	(nil, rem0) := str->splitl(s, " \t");
	rem0 = str->drop(rem0, " \t");
	(st, nil) := str->splitl(rem0, " \t");
	case st {
	"" =>	return (nil, "bad http response");
	* =>	return (nil, "fetch failed: "+nocrlf(rem0));
	"200" =>
		{} # ok
	}

	for(;;)
	case s = b.gets('\n') {
	"" =>
		return (nil, sprint("read http headers: %r"));
	"\n" or
	"\r\n" =>
		{
			c := xbrc(nm, b);
			c.url = u.text();
			return (c, nil);
		} exception ex {
		"x:*" =>
			return (nil, "parsing config: "+ex[2:]);
		}
	* =>
		{}  # header, ignore
	}
}

fetch9p(u: ref Url, nm: string): (ref Cfg, string)
{
	addr := dial->netmkaddr(u.addr, "net", "styx");
	cc := dial->dial(addr, nil);
	if(cc == nil)
		return (nil, sprint("dial: %r"));
	if(sys->mount(cc.dfd, nil, "/env", sys->MREPL, nil) < 0)
		return (nil, sprint("mount: %r"));
	err: string;
	c: ref Cfg;
	{
		b := bufio->open("/env/"+u.path, bufio->OREAD);
		if(b == nil)
			raise sprint("x:open: %r");
		c = xbrc(nm, b);
	} exception ex {
	"x:*" =>
		err = "parsing config: "+ex[2:];
	}
	if(sys->unmount(sys->fd2path(cc.dfd), "/env") < 0)
		warn(sprint("unmount: %r"));
	return (c, err);
}

fetch(u: ref Url, nm: string): (ref Cfg, string)
{
	case u.proto {
	"http" =>	return fetchhttp(u, nm);
	"9p" =>		return fetch9p(u, nm);
	}
	return (nil, "unknown proto");
}

cs()
{
	cs := load Cmd "/dis/ndb/cs.dis";
	if(cs == nil)
		fail(sprint("load ndb/cs: %r"));
	cs->init(nil, list of {"/ndb/cs"});
}

run(c: ref Cfg, ppid: int)
{
	tkmsg("inferno is being loaded.\n\nthis user interface has been killed and will be replaced by inferno's window manager shortly.\nif this is the first run it may take a while before the interface shows.\n\nenjoy!\n");
	tkcmd("update");

	# kill the gui, make sure /dev/pointer is available again (fd holding it must be gc'ed)
	killgrp(ppid);
	for(;;) {
		fd := sys->open("/dev/pointer", sys->OREAD);
		if(fd != nil) {
			fd = nil;
			break;
		}
		warn("/dev/pointer not yet available...");
		sys->sleep(100);
	}
	cs();

	remaddr := dial->netmkaddr(c.venti, "tcp", "17034");
	writeaddr := "tcp!localhost!57034";
	localaddr := "tcp!localhost!47034";

say("tventi");
	tventi := load Cmd "/dis/tventi.dis";
	if(tventi == nil)
		fail(sprint("load tventi: %r"));
	tventi->init(nil, list of {"tventi", "-dV", "-f", "/vacinit/lib/vacinit/tdata", "-i", "/vacinit/lib/vacinit/tindex", "-c", "-a", writeaddr});

say("xventi");
	xventi := load Cmd "/dis/xventi.dis";
	if(xventi == nil)
		fail(sprint("load xventi: %r"));
	xventi->init(nil, list of {"xventi", "-d", "-a", localaddr, remaddr, writeaddr});

	# keep tventi & xventi in their original namespace
	sys->pctl(Sys->FORKNS, nil);

	if(sys->chdir("/") < 0)
		fail(sprint("chdir /: %r"));

say("vacsrv");
	vacsrv := load Cmd "/dis/vacsrv.dis";
	if(vacsrv == nil)
		fail(sprint("load vacsrv: %r"));
	vacsrv->init(nil, list of {"vacsrv", "-n", "-a", localaddr, "-m", "/", c.score});
	xbind("#/", "/", sys->MBEFORE);
say("new root");

	xbind("#^", "/dev", Sys->MBEFORE);
	xbind("#^", "/chan", Sys->MBEFORE);
	xbind("#m", "/dev", Sys->MBEFORE);
	xbind("#c", "/dev", Sys->MBEFORE);
	xbind("#p", "/prog", Sys->MREPL);
	xbind("#d", "/fd", Sys->MREPL);
	xbind("#I", "/net", Sys->MREPL);
	xbind("#e", "/env", Sys->MREPL|sys->MCREATE);
	xbind("#scs", "/net", Sys->MBEFORE);
	xbind(wdir, "/local", Sys->MREPL|sys->MCREATE);

	wf("/env/vaclocalwrite", writeaddr);
	wf("/env/vacproxy", localaddr);
	l := ref ("score", c.score)::ref ("venti", c.venti)::c.env;
	if(c.url != nil)
		l = ref ("url", c.url)::l;
	if(c.walk != nil)
		l = ref ("walk", c.walk)::l;
	if(c.run != nil)
		l = ref ("run", c.run)::l;
	for(; l != nil; l = tl l) {
		(k, v) := *hd l;
		wf(sprint("/env/vac%s", k), v);
	}

	if(c.walk != nil)
		vacwalk(c, localaddr, writeaddr);

	sh = load Sh Sh->PATH;
	if(sh == nil)
		fail(sprint("load sh: %r"));
	run := c.run;
	if(run == nil)
		run = "/lib/vacprofile";
	sh->system(nil, sprint("sh %q", run));
}

vacwalk(c: ref Cfg, localaddr, writeaddr: string)
{
say("vacwalk...");
	# dial venti server where mark may be stored
	wc := dial->dial(writeaddr, nil);
	if(wc == nil)
		return warn(sprint("dial: %r"));
	ws := Session.new(wc.dfd);
	if(ws == nil)
		return warn(sprint("venti session to writeaddr %q: %r", writeaddr));

	# paths to fetch from vacwalk, ensure it ends with a newline
	fd := sys->open("/00vacpaths", sys->OREAD);
	if(fd == nil)
		return say(sprint("no /00vacpaths: %r"));
	(ok, dir) := sys->fstat(fd);
	if(ok != 0)
		return warn(sprint("fstat /00vacpaths: %r"));
	if(sys->readn(fd, d := array[int dir.length+1] of byte, int dir.length) != int dir.length)
		return warn(sprint("read /00vacpaths: %r"));
	o := int dir.length;
	if(d[o-1] != byte '\n')
		d[o++] = byte '\n';
	d = d[:o];

	# see if mark is in local venti server.  if so, we already have the blocks we need
	(nil, s) := Score.parse(c.score);
	mark := array[2*Scoresize] of byte;
	mark[:] = sha1(d);
	mark[Scoresize:] = s.a;
	x := ws.read(Score(sha1(mark)), venti->Datatype, len mark);
	if(x != nil)
		return say("already vacwalked");

	# dial local (proxy) venti server to read scores through
	lc := dial->dial(localaddr, nil);
	if(lc == nil)
		return warn(sprint("dial: %r"));
	ls := Session.new(lc.dfd);
	if(ls == nil)
		return warn(sprint("venti session to localaddr %q: %r", localaddr));

	# request paths from vacwalk
	addr := dial->netmkaddr(c.walk, "net", "5657");
	vc := dial->dial(addr, nil);
	if(vc == nil)
		return warn(sprint("dial %q: %r", addr));
	if(sys->fprint(vc.dfd, "%s\n%s\n", c.score, string d) < 0)
		return warn(sprint("write vacwalk request: %r"));
say("sent vacwalks request");

	# set up for reading scores from vacwalk and fetching them
	pidc := chan of int;
	errc := chan of string;
	sc := chan[1] of ref (int, Score);
	tc := chan[256] of ref Vmsg;
	rc := chan[1] of ref Vmsg;
	spawn scoreread(vc.dfd, sc, pidc, errc);
	p0 := <-pidc;
	spawn vwrite(lc.dfd, tc, pidc, errc);
	spawn vread(lc.dfd, rc, pidc, errc);
	p1 := <-pidc;
	p2 := <-pidc;

	tids: list of int;
	for(i := 0; i < 256; i++)
		tids = i::tids;
	work: list of ref (int, Score);
	seen := Strhash[string].new(31, nil); # type+score as strings
	done := 0;
	err: string;
	while(err == nil) {
		alt {
		err = <-errc =>
			continue;

		t := <-sc =>
			if(t == nil)
				done = 1;
			else if(seen.find(ss := sprint("%d%s", t.t0, t.t1.text())) == nil) {
				seen.add(ss, ss);
				work = t::work;
			}

		mm := <-rc =>
			# we don't care for the results
			tids = mm.tid::tids;
		}
		if(done && len tids == 256 && work == nil)
			break;
		while(work != nil && tids != nil) {
			(t, ss) := *hd work;
			tid := hd tids;
			tc <-= ref Vmsg.Tread(1, tid, ss, t, venti->Maxlumpsize);
			work = tl work;
			tids = tl tids;
		}
	}
	if(!done)
		kill(p0);
	kill(p1);
	kill(p2);

say(sprint("vacwalk done, err %#q", err));
	if(err != nil)
		return warn(err);

	(ok, nil) = ws.write(venti->Datatype, mark);
	if(ok < 0)
		return warn(sprint("write vacwalk mark: %r"));
say("vacwalk completed");
}

scoreread(fd: ref Sys->FD, c: chan of ref (int, Score), pidc: chan of int, errc: chan of string)
{
	pidc <-= pid();
	b := bufio->fopen(fd, bufio->OREAD);
	for(;;) {
		s := b.gets('\n');
		if(s == nil)
			break;
		if(s[len s-1] != '\n') {
			errc <-= "missing newline from vacwalk";
			return;
		}
		l := sys->tokenize(s[:len s-1], " ").t1;
		if(len l != 2) {
			errc <-= "malformed line from vacwalk";
			return;
		}
		(t, rt) := str->toint(hd l, 10);
		if(rt != nil || t <= venti->Errtype || t >= venti->Maxtype) {
			errc <-= "bad type from vacwalk";
			return;
		}
		(ok, sc) := Score.parse(hd tl l);
		if(ok < 0) {
			errc <-= "bad score from vacwalk";
			return;
		}
		c <-= ref (t, sc);
	}
	c <-= nil;
}

vwrite(fd: ref Sys->FD, c: chan of ref Vmsg, pidc: chan of int, errc: chan of string)
{
	pidc <-= pid();
	for(;;) {
		m := <-c;
		if(sys->write(fd, d := m.pack(), len d) != len d)
			errc <-= sprint("write to venti: %r");
	}
}

vread(fd: ref Sys->FD, c: chan of ref Vmsg, pidc: chan of int, errc: chan of string)
{
	pidc <-= pid();
	for(;;) {
		(m, err) := Vmsg.read(fd);
		if(err != nil) {
			errc <-= "read from venti: "+err;
			return;
		}
		c <-= m;
	}
}

rc(nm: string): (ref Cfg, string)
{
	{
		return (xrc(nm), nil);
	} exception ex {
	"x:*" =>
		return (nil, ex[2:]);
	}
}

xrc(nm: string): ref Cfg
{
	f := "/vacinit/lib/vacinit/"+nm;
	b := bufio->open(f, bufio->OREAD);
	if(b == nil)
		raise sprint("x:open: %r");
	return xbrc(nm, b);
}

xbrc(nm: string, b: ref Iobuf): ref Cfg
{
	c := ref Cfg;
	c.name = nm;
	for(;;) {
		s := b.gets('\n');
		if(s == nil)
			break;
		if(s[len s-1] == '\n')
			s = s[:len s-1];
		l := str->unquoted(s);
		if(str->prefix("#", str->drop(s, " \t")) || l == nil)
			continue;
		if(len l != 1)
			raise sprint("x:bad line, multiple tokens");
		(k, v) := str->splitl(hd l, "=");
		if(v == nil)
			raise sprint("x:missing =, not assignment");
		v = v[1:];
		case k {
		"score" =>	c.score = v;
		"venti" =>	c.venti = v;
		"url" =>	c.url = v;
		"walk" =>	c.walk = v;
		"run" =>	c.run = v;
		* =>
			c.env = ref (k, v)::c.env;
		}
	}
	if(c.score == nil || c.venti == nil)
		raise "x:no score or venti server specified";
	return c;
}

Cfg.text(c: self ref Cfg): string
{
	s := sprint("score=%q\nventi=%q\n", c.score, c.venti);
	if(c.url != nil)
		s += sprint("url=%q\n", c.url);
	if(c.walk != nil)
		s += sprint("walk=%q\n", c.walk);
	if(c.run != nil)
		s += sprint("run=%q\n", c.run);
	for(l := c.env; l != nil; l = tl l)
		s += sprint("%q=%q\n", (hd l).t0, (hd l).t1);
	return s;
}

wc(c: ref Cfg): string
{
	d := array of byte c.text();
	f := "/vacinit/lib/vacinit/"+c.name;
	fd := sys->create(f, sys->OWRITE, 8r666);
	if(fd == nil || sys->write(fd, d, len d) != len d)
		err := sprint("write: %r");
	return err;
}

xbind(src, dst: string, fl: int)
{
	if(sys->bind(src, dst, fl) < 0)
		warn(sprint("bind %q %q: %r", src, dst));
}

tkcmd(s: string): string
{
	r := tk->cmd(top, s);
	if(dflag > 1 || r != nil && r[0] == '!')
		warn(sprint("tkcmd: %s -> %s", s, r));
	return r;
}

wf(f, s: string)
{
	fd := sys->create(f, sys->OWRITE, 8r666);
	if(fd == nil || sys->fprint(fd, "%s", s) < 0)
		fail(sprint("%q: %r", f));
}

tkmsg(s: string)
{
	if(s != nil)
		warn(s);
	tkcmd(".m.txt delete 1.0 end");
	tkcmd(".m.txt insert 1.0 '"+s);
}

sha1(d: array of byte): array of byte
{
	dig := array[kr->SHA1dlen] of byte;
	kr->sha1(d, len d, dig, nil);
	return dig;
}

l2a[T](l: list of T): array of T
{
	a := array[len l] of T;
	i := 0;
	for(; l != nil; l = tl l)
		a[i++] = hd l;
	return a;
}

rev[T](l: list of T): list of T
{
	r: list of T;
	for(; l != nil; l = tl l)
		r = hd l::r;
	return r;
}

suffix(suf, s: string): int
{
	return len suf <= len s && suf == s[len s-len suf:];
}

progctl(pid: int, s: string)
{
	sys->fprint(sys->open(sprint("#p/%d/ctl", pid), Sys->OWRITE), "%s", s);
}

killgrp(pid: int)
{
	progctl(pid, "killgrp");
}

kill(pid: int)
{
	progctl(pid, "kill");
}

pid(): int
{
	return sys->pctl(0, nil);
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
