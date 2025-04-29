<?php

namespace Basis\Sharding\Test\Entity;

use Tarantool\Mapper\Space;

class MapperLogin
{
    public function __construct(
        public int $id,
        public string $username,
        public string $password,
    ) {
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['username'], ['unique' => true]);
    }
}
