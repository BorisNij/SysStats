#!/usr/bin/env python3

import asyncio
import argparse
import json
from typing import List, Optional, Union


class CommandRunException(Exception):
    pass


class UserStat:
    def __init__(
        self,
        username: str,
        upn: Optional[str] = None,
        rss_total: int = 0,
        pmem_total: float = 0.0,
        pcpu_total: float = 0.0,
    ):
        self.username = username
        self.upn = upn
        self.rss_total = rss_total
        self.pmem_total = pmem_total
        self.pcpu_total = pcpu_total

    def to_table_row(self) -> str:
        upn = self.upn if self.upn else "none"
        return f"{self.username}\t{upn}\t{self.rss_total}\t{self.pmem_total:.2f}\t{self.pcpu_total:.2f}"

    def to_json(self) -> str:
        return json.dumps(self.__dict__)

async def run_command(command: str) -> str:
    process = await asyncio.create_subprocess_shell(
        command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    output, error = await process.communicate()
    if process.returncode != 0:
        raise CommandRunException(f"Error running command {command}: {error.decode()}")
    return output.decode()

async def get_user_stats(username: str) -> Optional[UserStat]:
    try:
        upn = await run_command(f"adquery user -P {username}")
    except CommandRunException:
        upn = None

    try:
        process_stats = await run_command(
            f"ps -hax -o rss,pmem,pcpu -u {username}"
        )
        rss_total, pmem_total, pcpu_total = 0, 0.0, 0.0
        for line in process_stats.strip().split("\n"):
            rss, pmem, pcpu = line.split()
            rss_total += int(rss)
            pmem_total += float(pmem)
            pcpu_total += float(pcpu)
        return UserStat(username, upn.strip() if upn else None, rss_total, pmem_total, pcpu_total)
    except CommandRunException:
        return None


def print_stats(stats: List[UserStat], output_format: str):
    if output_format == "table":
        print("Username\tUPN\tRSS Total\tPMEM Total\tPCPU Total")
        for stat in stats:
            print(stat.to_table_row())
    elif output_format == "json":
        print(json.dumps([stat.__dict__ for stat in stats]))


async def main(output_format: str = "table"):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "output_format",
        nargs="?",
        choices=["table", "json"],
        default=output_format,
        help="Output format of the user stats",
    )
    args = parser.parse_args()

    usernames = await run_command("adquery group -m")
    user_stats = await asyncio.gather(
        *(get_user_stats(username.strip()) for username in usernames.strip().split("\n"))
    )

    print_stats([stat for stat in user_stats if stat], args.output_format)


if __name__ == "__main__":
    asyncio.run(main())
