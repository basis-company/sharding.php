<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Router;

interface Bootstrap
{
    public static function bootstrap(Router $router): void;
}
