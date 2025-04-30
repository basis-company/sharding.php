<?php

namespace Basis\Sharding\Entity;

use Tarantool\Mapper\Space;

class Change
{
    public function __construct(
        public int $lsn,
        public string $table,
        public string $operation,
        public array $tuple,
    ) {
    }

    public static function getSpaceName(): string
    {
        return "sharding_change";
    }

    public static function initSchema(Space $space)
    {
    }
}
