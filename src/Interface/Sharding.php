<?php

namespace Basis\Sharding\Interface;

interface Sharding
{
    public static function getKey(array $data): int|string|null;
}
