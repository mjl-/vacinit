implement WmSecstore;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
	draw: Draw;
include "arg.m";
include "dial.m";
	dial: Dial;
include "string.m";
	str: String;
include "secstore.m";
	secstore: Secstore;
include "tk.m";
	tk: Tk;
include "tkclient.m";
	tkclient: Tkclient;

WmSecstore: module {
	init:	fn(ctxt: ref Draw->Context, argv: list of string);
};

addr := "$auth";
user := "";
file := "factotum";
password: string;

top: ref Tk->Toplevel;

init(ctxt: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	if(ctxt == nil)
		fail("no window context");
	draw = load Draw Draw->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	secstore = load Secstore Secstore->PATH;
	secstore->init();
	tk = load Tk Tk->PATH;
	tkclient = load Tkclient Tkclient->PATH;

	sys->pctl(sys->NEWPGRP, nil);

	secstore->privacy();

	arg->init(args);
	arg->setusage(arg->progname()+" [-s addr] [-u user] [-f file]");
	while((c := arg->opt()) != 0)
		case c {
		's' =>	addr = arg->earg();
		'u' =>	user = arg->earg();
		'f' =>	file = arg->earg();
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 0)
		arg->usage();

	if(user == nil)
		user = readuser();

	tkclient->init();
	wmctl: chan of string;
	(top, wmctl) = tkclient->toplevel(ctxt, "", "secstore", Tkclient->Appl);

	cmdc := chan of string;
	passkeyc := chan of string;
	tk->namechan(top, cmdc, "cmd");
	tk->namechan(top, passkeyc, "passkey");

	tkcmd("frame .g");
	tkcmd("label .g.luser -text user");
	tkcmd("label .g.laddr -text address");
	tkcmd("label .g.lpass -text password");
	tkcmd("label .g.lfile -text file");
	tkcmd("entry .g.euser; .g.euser insert 0 '"+user);
	tkcmd("entry .g.eaddr; .g.eaddr insert 0 '"+addr);
	tkcmd("entry .g.epass -show *");
	tkcmd("entry .g.efile; .g.efile insert 0 '"+file);
	tkcmd("text .g.msg -state disabled -wrap word");

	tkcmd("grid .g.laddr	-row 0 -column 0 -sticky w");
	tkcmd("grid .g.eaddr	-row 0 -column 1 -sticky we");
	tkcmd("grid .g.luser	-row 1 -column 0 -sticky w");
	tkcmd("grid .g.euser	-row 1 -column 1 -sticky we");
	tkcmd("grid .g.lpass	-row 2 -column 0 -sticky w");
	tkcmd("grid .g.epass	-row 2 -column 1 -sticky we");
	tkcmd("grid .g.lfile	-row 3 -column 0 -sticky w");
	tkcmd("grid .g.efile	-row 3 -column 1 -sticky we");
	tkcmd("grid .g.msg	-row 4 -columnspan 2 -sticky nswe");
	tkcmd("grid columnconfigure .g 1 -weight 1");
	tkcmd("pack .g -fill both -expand 1");
	
	tkcmd("bind .g.epass <Key> {send passkey %K}");
	tkcmd("bind .g.epass <Control-u> {send passkey %K}");
	tkcmd("bind .g.epass <Control-h> {send passkey %K}");
	ww := array[] of {".g.eaddr", ".g.euser", ".g.epass", ".g.efile"};
	for(i := 0; i < len ww; i++) {
		tkcmd(sprint("bind %s {<Key-\n>} {send cmd get}", ww[i]));
		nw := ww[(i+1)%len ww];
		tkcmd(sprint("bind %s {<Key-\t>} {%s selection clear; focus %s; %s selection range 0 end}", ww[i], ww[i], nw, nw));
	}
	tkcmd("bind .g.epass <FocusIn> {send cmd passin}");
	tkcmd("bind .g.epass <Enter> {send cmd passin}");
	tkcmd("bind .g.epass <FocusOut> {send cmd passout}");
	tkcmd("bind .g.epass <Leave> {send cmd passout}");
	tkcmd("focus .g.eaddr; .g.eaddr selection range 0 end");
	tkcmd("pack propagate . 0");
	tkcmd(". configure -width 70w");

	tkmsg("\nTab cycles focus.\nNewline logs in, fetches file and writes it to stdout line by line.\n");

	tkclient->onscreen(top, nil);
	tkclient->startinput(top, "kbd"::"ptr"::nil);

	for(;;) alt {
	s := <-top.ctxt.kbd =>
		tk->keyboard(top, s);

	s := <-top.ctxt.ptr =>
		tk->pointer(top, *s);

	s := <-top.ctxt.ctl or
	s = <-top.wreq or
	s = <-wmctl =>
		if(s == "exit")
			erasepass();
		tkclient->wmctl(top, s);

	s := <-passkeyc =>
		Ctl: con -16r60;
		(cc, rm) := str->toint(s, 16);
		if(rm != nil) {
			warn(sprint("bad key %q, remaining %q", s, rm));
			continue;
		}
		case cc {
		Ctl+'h' =>
			if(password != nil) {
				password[len password-1] = 0;
				password = password[:len password-1];
			}
		Ctl+'u' =>
			erasepass();
		* =>
			password[len password] = cc;
		}
		cc = 0;

	s := <-cmdc =>
		case s {
		"get" =>
			tkmsg("");
			user = tkcmd(".g.euser get");
			addr = tkcmd(".g.eaddr get");
			file = tkcmd(".g.efile get");
			if(user == nil || addr == nil || file == nil) {
				tkmsg("empty user, addr, file");
				break;
			}

			done := 0;
			seckey := secstore->mkseckey(password);
			filekey := secstore->mkfilekey(password);
			erasepass();
			if(seckey == nil)
				tkmsg(sprint("mkseckey: %r"));
			else if(filekey == nil)
				tkmsg(sprint("mkfilekey: %r"));
			else {
				err := get(seckey, filekey);
				tkmsg(err);
				done = err==nil;
			}
			secstore->erasekey(seckey);
			secstore->erasekey(filekey);
			if(done)
				return;
		"passin" =>
			msg := "\n";
			msg += "Password is not echoed.\n";
			msg += "Ctrl-u is line kill.\n";
			msg += "Ctrl-h is character erase.\n";
			msg += "Tab or newline cannot be part of password.\n";
			tkmsg(msg);
		"passout" =>
			tkmsg("");
		}
		tkcmd("update");
	}
}

erasepass()
{
	n := len password;
	for(i := 0; i < n; i++)
		password[i] = 0;
	password = "";
}

get(seckey, filekey: array of byte): string
{
	secaddr := dial->netmkaddr(addr, nil, "secstore");
	(conn, remname, err) := secstore->connect(secaddr, user, seckey);
	if(conn == nil)
		return "connecting: "+err;
	tkmsg(r := sprint("logged into %q at %q\n", remname, secaddr));

	cipher := secstore->getfile(conn, file, secstore->Maxfilesize);
	if(cipher == nil)
		return r += sprint("getting file %#q: %r\n", file);

	plain := secstore->decrypt(cipher, filekey);
	if(plain == nil) {
		secstore->erasekey(cipher);
		return r += sprint("decrypt: %r");
	}

	# erasing plain lines, the cipher and plain text erases the same buffer.
	# at least according to documentation.  just being cautious.
	fd1 := sys->fildes(1);
	for(ll := secstore->lines(plain); ll != nil; ll = tl ll) {
		d := hd ll;
		if(sys->write(fd1, d, len d) != len d)
			warn(sprint("write: %r"));
		secstore->erasekey(d);
	}
	secstore->erasekey(cipher);
	secstore->erasekey(plain);
	return nil;
}

tkmsg(s: string)
{
	tkcmd(".g.msg delete 1.0 end");
	if(s != nil)
		tkcmd(".g.msg insert 1.0 '"+s);
}

readuser(): string
{
	fd := sys->open("/dev/user", sys->OREAD);
	if(fd != nil)
	if((n := sys->readn(fd, d := array[128] of byte, len d)) > 0)
		return string d[:n];
	return "";
}

tkcmd(s: string): string
{
	r := tk->cmd(top, s);
	if(r != nil && r[0] == '!')
		warn(sprint("tkcmd: %q -> %q", s, r));
	return r;
}

warn(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}
