<?php

namespace Basis\Sharding\Schema;

use Tarantool\Mapper\Entity;
use Tarantool\Mapper\Repository;

final class Legacy
{
    public static function initialize()
    {
        if (!class_exists(Entity::class, false)) {
            eval("namespace Tarantool\\Mapper; class Entity {}");
        }

        if (!class_exists(Repository::class, false)) {
            eval("namespace Tarantool\\Mapper; class Repository {}");
        }
    }
}
