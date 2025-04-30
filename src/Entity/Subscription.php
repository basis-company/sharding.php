<?php

namespace Basis\Sharding\Entity;

class Subscription
{
    public function __construct(
        public int $id,
        public string $listener,
        public string $table,
    ) {
    }

    public static function getSpaceName(): string
    {
        return "sharding_subscription";
    }
}
