"""Reject common accidental full-command process wait loops before Bash runs.

A correctness guard, not a shell security sandbox: it never executes or rewrites
commands, terminates processes, or declares tests complete.
"""
import re
import shlex
from dataclasses import dataclass
from pathlib import PurePosixPath

REASON = ("Deze wachtlus gebruikt pgrep met volledige commandoregels en kan zichzelf of ander werk blijven vinden. "
          "Gebruik de bestaande Bash-achtergrondtaak met run_in_background en lees het resultaat via TaskOutput "
          "met de verkregen task_id; start geen tweede testrun. Of voer de test op de voorgrond uit met een "
          "begrensde timeout. Een proceszoekopdracht is geen bewijs van testvoltooiing.")
SEPARATORS = {';', ';;', '&', '&&', '|', '||', '(', ')', '\n'}

@dataclass(frozen=True)
class _Token:
    word: str
    operator: bool = False
    literal: bool = False
    unquoted: bool = True

def _lex(text):
    # Non-POSIX mode preserves quotes, including multiline single-quoted literal
    # arguments. Decode words separately without losing their expansion context.
    lex=shlex.shlex(text,posix=False,punctuation_chars=';&|()<>\n')
    lex.whitespace=' \t\r';lex.whitespace_split=True
    tokens=[]
    for raw in lex:
        if all(c in ';&|()<>\n' for c in raw):
            tokens.extend(_Token(p,operator=True) for p in re.findall(r'<<<|&&|\|\||;;|<<|>>|.',raw,re.S))
        else:
            try:words=shlex.split(raw)
            except ValueError:words=[raw]
            word=words[0] if len(words)==1 else raw
            tokens.append(_Token(word,literal=raw.startswith("'"),unquoted=raw==word))
    return tokens

def _tokens(command):
    # Join escaped physical lines before tokenization. Keep lexical state across
    # multiline quotes, and remove here-document data before parsing commands.
    command=command.replace('\\\n','')
    out=[];pending=[];header=''
    for line in command.splitlines(keepends=True):
        if pending:
            delimiter,strip_tabs=pending[0]
            actual=line.rstrip('\r\n')
            if strip_tabs:actual=actual.lstrip('\t')
            if actual==delimiter:pending.pop(0)
            continue
        out.append(line);header+=line
        try:row=_lex(header)
        except ValueError:continue
        for i,token in enumerate(row[:-1]):
            if token.operator and token.word=='<<':
                delimiter=row[i+1].word
                pending.append((delimiter[1:] if delimiter.startswith('-') else delimiter,delimiter.startswith('-')))
        header=''
    return _lex(''.join(out))

def unsafe_process_wait(command, *, _in_loop=False, _depth=0):
    if not isinstance(command,str) or _depth>5:return False
    try:tokens=_tokens(command)
    except ValueError:return False  # Let Bash report malformed shell syntax.
    loops=int(_in_loop);position=True
    for i,token in enumerate(tokens):
        word=token.word
        if token.operator and word in SEPARATORS:
            position=True;continue
        if position and token.unquoted and word in {'while','until'}:
            loops+=1;continue
        if position and token.unquoted and word=='done':
            loops=max(int(_in_loop),loops-1);position=False;continue
        if position and token.unquoted and word in {'!','do','then','if','elif','else','{'}:continue
        if loops and not token.literal:
            for inner in re.findall(r'\$\(([^()]*)\)',word):
                if unsafe_process_wait(inner,_in_loop=True,_depth=_depth+1):return True
        if position and PurePosixPath(word).name=='pgrep' and loops:
            for arg in tokens[i+1:]:
                if arg.operator and arg.word in SEPARATORS:break
                if arg.word=='--full' or (arg.word.startswith('-') and not arg.word.startswith('--') and 'f' in arg.word[1:]):return True
        if position and PurePosixPath(word).name in {'sh','bash','dash','zsh'}:
            for offset,arg in enumerate(tokens[i+1:i+4],i+1):
                if arg.word.startswith('-') and 'c' in arg.word[1:] and offset+1<len(tokens):
                    if unsafe_process_wait(tokens[offset+1].word,_in_loop=bool(loops),_depth=_depth+1):return True
                    break
        position=False
    return False
