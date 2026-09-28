"""负载均衡：在健康实例间做轮询。

实现要点：
- 每条路由独立游标；
- 输入为调用方按配置顺序过滤后的健康实例列表（不健康实例已摘除），
  因此“不会把请求持续打到已判定不健康的实例”由健康过滤保证；
- 用健康列表在配置顺序中的位置做取模，实例临时摘挂不会打乱其它实例
  之间的轮询节奏，结果稳定可复现。
"""

from __future__ import annotations

import threading


class RoundRobinBalancer:
    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._order: dict[str, list[str]] = {}
        self._lock = threading.Lock()

    def choose(self, route_id: str, order: list[str], healthy: list[str]) -> str | None:
        """从 healthy 中按轮询选一个。

        order: 该路由配置的完整实例顺序（稳定下标来源）；
        healthy: 当前健康实例（顺序需与 order 一致）。
        """
        if not healthy:
            return None
        healthy_set = set(healthy)
        with self._lock:
            n = self._counters.get(route_id, 0)
            eligible = [uid for uid in order if uid in healthy_set]
            chosen = eligible[n % len(eligible)]
            self._counters[route_id] = n + 1
            self._order[route_id] = order
            return chosen
