<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Database;

interface Job
{
    public function __invoke(Database $crud);
}
