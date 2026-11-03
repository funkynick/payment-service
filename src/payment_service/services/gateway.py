"""Эмуляция внешнего платёжного шлюза: 2-5 секунд, 90% успех / 10% отказ."""

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass
from decimal import Decimal

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class GatewayResult:
    reason: str | None = None  # None = успех

    @property
    def success(self) -> bool:
        return self.reason is None


class PaymentGatewayEmulator:
    def __init__(
        self,
        delay_min: float,
        delay_max: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._delay_min = delay_min
        self._delay_max = delay_max
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment_id: uuid.UUID, amount: Decimal, currency: str) -> GatewayResult:
        """Отказ шлюза - это бизнес-результат (платёж failed), а не исключение.
        Технические ошибки (сеть, таймаут) в реальной интеграции поднимались бы как
        исключения и уходили бы в retry."""
        log.debug("gateway charge %s %s %s", payment_id, amount, currency)
        await asyncio.sleep(self._rng.uniform(self._delay_min, self._delay_max))
        if self._rng.random() < self._success_rate:
            return GatewayResult()
        return GatewayResult(reason="declined_by_gateway")
