"""Windows-implementatie van procesidentiteit, procesoverzicht en stoppen (ctypes, alleen standaardbibliotheek).

Windows kent geen procesgroepen zoals POSIX: `pgid` is hier de pid van de wortel van de boom. Identiteit = pid +
aanmaaktijd (FILETIME), zodat een hergebruikte pid niet voor het oude proces doorgaat.

Integriteitsniveaus: een proces zonder verhoogde rechten (de watch, geplande taak met /RL LIMITED) krijgt op een proces
met verhoogde rechten (een taakproces dat via ssh als beheerder startte) bij `OpenProcess` *Access denied*. Daarom komen
pid, ouder-pid en aanmaaktijd uit `NtQuerySystemInformation(SystemProcessInformation)`: die lijst vraagt geen handle en
toont alle processen, ongeacht hun rechten. Lukt zelfs dat niet, dan geldt een bestaande maar ontoegankelijke pid als
`UNKNOWN` (leeft, identiteit onbekend) en nooit als 'weg'.

Stoppen gebeurt per proces via een handle waarop eerst de aanmaaktijd gecontroleerd wordt (`kill_exact`): de handle
houdt het procesobject vast, dus tussen controle en stoppen kan de pid niet aan een ander proces zijn gegeven. Er wordt
geen `taskkill /T` meer gebruikt; die volgt de ouder-pid blind en raakt zo ook processen met een hergebruikte ouder-pid.

Alle ctypes-functies zijn alleen op Windows aanroepbaar; de pure hulpfuncties (`filetime_text`,
`parse_process_info`) werken overal, zodat ze getest kunnen worden.
"""
import os
import struct
import sys

WINDOWS = sys.platform == "win32"
PROCESS_TERMINATE = 0x0001
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
STILL_ACTIVE = 259
TH32CS_SNAPPROCESS = 0x2
CMDLINE_INFO_CLASS = 60   # ProcessCommandLineInformation (Windows 8.1+)
SYSTEM_PROCESS_INFORMATION = 5
STATUS_INFO_LENGTH_MISMATCH = 0xC0000004
ERROR_ACCESS_DENIED = 5
ERROR_INVALID_PARAMETER = 87   # OpenProcess op een pid die niet bestaat
INVALID_HANDLE = -1
UNKNOWN = "?"   # proces bestaat, maar de aanmaaktijd is niet te bepalen: telt als levend, nooit als 'weg'

# Offsets in SYSTEM_PROCESS_INFORMATION per pointergrootte: (NumberOfThreads, CreateTime, ImageName.Length,
# ImageName.Buffer, UniqueProcessId, InheritedFromUniqueProcessId).
_SPI = {8: (4, 32, 56, 64, 80, 88), 4: (4, 32, 56, 60, 68, 72)}


def filetime_text(low: int, high: int) -> str:
    """Aanmaaktijd (FILETIME, 100 ns sinds 1601) als stabiele decimale tekst."""
    return str((high << 32) | low)


def parse_process_info(buf: bytes, base: int, ptr_size: int = 8) -> list:
    """Records uit een SystemProcessInformation-buffer -> [{pid, ppid, start, threads, image}].

    `base` is het geheugenadres van `buf`: de naam van het image staat in dezelfde buffer en de kernel geeft er een
    pointer naar. Een record met 0 threads is een afgelopen proces waar nog een handle op openstaat ('zombie').
    """
    o_thr, o_ct, o_len, o_buf, o_pid, o_ppid = _SPI[ptr_size]
    ptr = "<Q" if ptr_size == 8 else "<I"
    rows, off = [], 0
    while off + o_ppid + ptr_size <= len(buf):
        nxt, threads = struct.unpack_from("<II", buf, off)
        (ct,) = struct.unpack_from("<q", buf, off + o_ct)
        (nlen,) = struct.unpack_from("<H", buf, off + o_len)
        (nptr,) = struct.unpack_from(ptr, buf, off + o_buf)
        (pid,) = struct.unpack_from(ptr, buf, off + o_pid)
        (ppid,) = struct.unpack_from(ptr, buf, off + o_ppid)
        image = ""
        rel = nptr - base
        if nptr and 0 <= rel and rel + nlen <= len(buf):
            image = bytes(buf[rel:rel + nlen]).decode("utf-16-le", errors="replace")
        rows.append({"pid": pid, "ppid": ppid, "start": str(ct), "threads": threads, "image": image})
        if not nxt:
            break
        off += nxt
    return rows


if WINDOWS:
    import ctypes
    from ctypes import wintypes

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _nt = ctypes.WinDLL("ntdll")
    _k32.OpenProcess.restype = wintypes.HANDLE
    _k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]
    _k32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    _k32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    _k32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _k32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    _k32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    _nt.NtQuerySystemInformation.restype = ctypes.c_long
    _nt.NtQuerySystemInformation.argtypes = [ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong, ctypes.POINTER(ctypes.c_ulong)]

    class _ENTRY(ctypes.Structure):
        _fields_ = [("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("th32ProcessID", wintypes.DWORD),
                    ("th32DefaultHeapID", ctypes.c_size_t), ("th32ModuleID", wintypes.DWORD),
                    ("cntThreads", wintypes.DWORD), ("th32ParentProcessID", wintypes.DWORD),
                    ("pcPriClassBase", wintypes.LONG), ("dwFlags", wintypes.DWORD), ("szExeFile", wintypes.WCHAR * 260)]

    _k32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ENTRY)]
    _k32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ENTRY)]

    def _open(pid: int, access: int = PROCESS_QUERY_LIMITED_INFORMATION):
        h = _k32.OpenProcess(access, False, pid)
        return h or None

    def _start_of(h) -> str | None:
        code = wintypes.DWORD()
        if not _k32.GetExitCodeProcess(h, ctypes.byref(code)) or code.value != STILL_ACTIVE:
            return None    # bestaat nog als handle maar is afgelopen
        c, e, k, u = (wintypes.FILETIME() for _ in range(4))
        if not _k32.GetProcessTimes(h, ctypes.byref(c), ctypes.byref(e), ctypes.byref(k), ctypes.byref(u)):
            return None
        return filetime_text(c.dwLowDateTime, c.dwHighDateTime)

    def _cmdline_of(h) -> str | None:
        size = wintypes.ULONG(0)
        _nt.NtQueryInformationProcess(h, CMDLINE_INFO_CLASS, None, 0, ctypes.byref(size))
        if not size.value:
            return None
        buf = ctypes.create_string_buffer(size.value)
        if _nt.NtQueryInformationProcess(h, CMDLINE_INFO_CLASS, buf, size, ctypes.byref(size)) != 0:
            return None
        # UNICODE_STRING { USHORT Length; USHORT MaximumLength; PWSTR Buffer } gevolgd door de tekst zelf.
        length = ctypes.c_ushort.from_buffer(buf).value
        ptr = ctypes.c_void_p.from_buffer(buf, ctypes.sizeof(ctypes.c_void_p)).value
        return ctypes.wstring_at(ptr, length // 2) if ptr else None

    def system_processes() -> list | None:
        """Alle processen uit de kernel (geen handle nodig, dus ook processen met verhoogde rechten); None bij een fout."""
        size = 1 << 20
        for _ in range(8):
            buf = ctypes.create_string_buffer(size)
            need = ctypes.c_ulong(0)
            status = _nt.NtQuerySystemInformation(SYSTEM_PROCESS_INFORMATION, buf, size, ctypes.byref(need)) & 0xFFFFFFFF
            if status == STATUS_INFO_LENGTH_MISMATCH:
                size = max(size * 2, need.value + 65536)
                continue
            if status != 0:
                return None
            return parse_process_info(buf.raw[:need.value or size], ctypes.addressof(buf), ctypes.sizeof(ctypes.c_void_p))
        return None

    def _last_error() -> int:
        return ctypes.get_last_error()

    def _close(h) -> None:
        _k32.CloseHandle(h)

    def _terminate_handle(h) -> bool:
        return bool(_k32.TerminateProcess(h, 1))

    def _toolhelp_rows() -> list:
        """Terugval als NtQuerySystemInformation faalt: Toolhelp-lijst, alleen processen die te openen zijn."""
        snap = _k32.CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0)
        if snap in (None, INVALID_HANDLE, ctypes.c_void_p(-1).value):
            return []
        rows = []
        try:
            e = _ENTRY()
            e.dwSize = ctypes.sizeof(_ENTRY)
            ok = _k32.Process32FirstW(snap, ctypes.byref(e))
            while ok:
                pid = e.th32ProcessID
                h = _open(pid) if pid else None
                if h:
                    try:
                        start = _start_of(h)
                    finally:
                        _k32.CloseHandle(h)
                    if start:
                        rows.append({"pid": pid, "ppid": e.th32ParentProcessID, "start": start, "threads": 1,
                                     "image": e.szExeFile})
                ok = _k32.Process32NextW(snap, ctypes.byref(e))
        finally:
            _k32.CloseHandle(snap)
        return rows


# Beslislogica hieronder is platformneutraal geschreven; de ruwe aanroepen (_open, _start_of, _cmdline_of, _close,
# _last_error, _terminate_handle, system_processes, _toolhelp_rows) bestaan alleen op Windows en worden in tests nagebootst.

def proc_start(pid: int) -> str | None:
    """Aanmaaktijd; None = bestaat niet (meer); UNKNOWN = bestaat, maar niet vast te stellen welk proces het is."""
    h = _open(pid)
    if h:
        try:
            return _start_of(h)
        finally:
            _close(h)
    if _last_error() == ERROR_INVALID_PARAMETER:
        return None
    rows = system_processes()   # meestal: Access denied op een proces met hogere rechten (watch zonder verhoging)
    if rows is None:
        return UNKNOWN
    row = next((r for r in rows if r["pid"] == pid), None)
    return row["start"] if row and row["threads"] else None


def snapshot() -> list:
    """Alle levende processen als {pid, ppid, pgid, start, cmd}; pgid is de pid zelf (geen procesgroepen).

    De opdrachtregel komt uit het proces zelf als dat te openen is; anders (hogere rechten) de naam van het image."""
    sysrows = system_processes()
    rows = []
    for r in sysrows if sysrows is not None else _toolhelp_rows():
        if not r["pid"] or not r["threads"]:
            continue
        cmd = None
        h = _open(r["pid"])
        if h:
            try:
                cmd = _cmdline_of(h)
            except OSError:
                cmd = None
            finally:
                _close(h)
        rows.append({"pid": r["pid"], "ppid": r["ppid"], "pgid": r["pid"], "start": r["start"],
                     "cmd": " ".join((cmd or r["image"] or "").split())[:200]})
    return rows


def kill_exact(pid: int, start: str | None) -> bool:
    """Stopt precies dit proces (pid + aanmaaktijd) via één handle; False als het niet (meer) dat proces is of als de
    rechten ontbreken. Zonder `start` (oude aanroepers) alleen als het proces nog leeft."""
    if pid <= 4 or pid == os.getpid():
        return False
    h = _open(pid, PROCESS_TERMINATE | PROCESS_QUERY_LIMITED_INFORMATION)
    if not h:
        return False
    try:
        now = _start_of(h)
        if now is None or (start and start != UNKNOWN and now != start):
            return False
        return _terminate_handle(h)
    finally:
        _close(h)


def terminate(pid: int, start: str | None = None) -> bool:
    """Beëindigt één proces onmiddellijk (TerminateProcess), alleen als het nog hetzelfde proces is."""
    return kill_exact(pid, start)
