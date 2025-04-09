<?php

namespace Basis\Sharded\Interface;

interface Sharding
{
    public static function getKey(array $data): ?string;
}
