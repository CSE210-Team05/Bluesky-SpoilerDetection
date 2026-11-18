"""Public spoiler detection worker and result interfaces."""

from .spoiler_interface import (
    ClaimedLabel,
    LabelJob,
    LabelQueue,
    PostIdentifier,
    SpoilerItem,
    SpoilerWorkerInterface,
)

__all__ = [
    "ClaimedLabel",
    "LabelJob",
    "LabelQueue",
    "PostIdentifier",
    "SpoilerItem",
    "SpoilerWorkerInterface",
]
