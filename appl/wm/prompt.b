implement Prompt;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
	draw: Draw;
include "arg.m";
include "keyboard.m";
include "tk.m";
	tk: Tk;
include "tkclient.m";
	tkclient: Tkclient;

Prompt: module {
	init:	fn(ctxt: ref Draw->Context, argv: list of string);
};

init(ctxt: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	if(ctxt == nil)
		fail("no window context");
	draw = load Draw Draw->PATH;
	arg := load Arg Arg->PATH;
	tk = load Tk Tk->PATH;
	tkclient = load Tkclient Tkclient->PATH;

	entrywidth := 30;
	value: string;
	qflag := 0;
	button := " ok ";

	sys->pctl(sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-q] [-b button] [-v value] [-w entrywidth] text");
	while((c := arg->opt()) != 0)
		case c {
		'b' =>	button = arg->earg();
		'q' =>	qflag++;
		'v' =>	value = arg->earg();
		'w' =>	entrywidth = int arg->earg();
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 1)
		arg->usage();

	tkclient->init();
	(top, wmctl) := tkclient->toplevel(ctxt, "", "prompt", Tkclient->Appl);

	cmdc := chan of string;
	tk->namechan(top, cmdc, "cmd");

	tk->cmd(top, "label .l -text '"+hd args);
	tk->cmd(top, sprint("entry .e -width %dw", entrywidth));
	if(value != nil)
		tk->cmd(top, ".e insert 0 '"+value);
	tk->cmd(top, "button .b -command {send cmd ok} -text '"+button);
	tk->cmd(top, "pack .l -side left");
	tk->cmd(top, "pack .e -side left -fill x -expand 1");
	tk->cmd(top, "pack .b -side left");
	tk->cmd(top, "focus .e");
	tk->cmd(top, "bind .e {<Key-\n>} {send cmd ok}");
	tk->cmd(top, sprint("bind .e <Key-%c> {send cmd abort}", Keyboard->Esc));

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
			raise "fail:aborted";
		tkclient->wmctl(top, s);

	s := <-cmdc =>
		case s {
		"ok" =>
			v := tk->cmd(top, ".e get");
			if(qflag)
				sys->print("%q", v);
			else
				sys->print("%s", v);
			return;
		"abort" =>
			raise "fail:aborted";
		}
	}
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
