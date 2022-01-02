from types import FrameType, TracebackType
from typing import Callable, List, Optional

_MODIFIED_EXCEPTION_VAR_NAME = "__modified_exception__"


def frames_to_traceback(
    frame: Optional[FrameType],
    limit: int,
    should_prune: Optional[Callable[[str], bool]] = None,
) -> Optional[TracebackType]:
    ctb: Optional[TracebackType] = None
    skipped = False
    while frame is not None and limit > 0:
        if _MODIFIED_EXCEPTION_VAR_NAME in frame.f_locals:
            return TracebackType(
                tb_next=None,
                tb_frame=frame,
                tb_lasti=frame.f_lasti,
                tb_lineno=frame.f_lineno,
            )
        if not skipped:
            if should_prune is not None and should_prune(frame.f_globals["__name__"]):
                frame = frame.f_back
                continue
            skipped = True
        if should_prune is None or not should_prune(frame.f_globals["__name__"]):
            ctb = TracebackType(
                tb_next=ctb,
                tb_frame=frame,
                tb_lasti=frame.f_lasti,
                tb_lineno=frame.f_lineno,
            )
            limit -= 1
            frame = frame.f_back
            continue
        break

    return ctb


def modify_traceback(
    traceback: Optional[TracebackType],
    should_prune: Optional[Callable[[str], bool]] = None,
    add_traceback: Optional[TracebackType] = None,
) -> Optional[TracebackType]:
    ctb: Optional[TracebackType] = None

    # get stack
    stack: List[TracebackType] = []

    if add_traceback is not None:
        f: Optional[TracebackType] = add_traceback
        while f is not None:
            stack.append(f)
            f = f.tb_next
    f = traceback
    while f is not None:
        stack.append(f)
        f = f.tb_next
    stack.reverse()

    # prune and reconstruct
    for n, f in enumerate(stack):
        if (
            n == 0
            or should_prune is None
            or not should_prune(f.tb_frame.f_globals["__name__"])
        ):
            ctb = TracebackType(
                tb_next=ctb,
                tb_frame=f.tb_frame,
                tb_lasti=f.tb_lasti,
                tb_lineno=f.tb_lineno,
            )

    return ctb
