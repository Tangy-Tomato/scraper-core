import ctypes

class MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]

def resolve_optimal_concurrency() -> int:
    """
    Evaluates physical memory against hardware reservation bands:
    - >= 14.5 GB -> 4 Browsers
    - >= 6.8 GB  -> 2 Browsers
    - Below 6.8  -> 1 Browser
    """
    stat = MEMORYSTATUSEX()
    stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
        return 1  # Fallback safely to 1 on kernel inspection failure

    ram_gb = stat.ullTotalPhys / (1024 ** 3)
    if ram_gb >= 14.5:
        return 4
    elif ram_gb >= 6.8:
        return 2
    else:
        return 1