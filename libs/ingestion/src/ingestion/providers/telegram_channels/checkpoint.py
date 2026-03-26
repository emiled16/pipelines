from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TelegramChannelsCheckpoint:
    channel_name: str
    last_entry_id: str

