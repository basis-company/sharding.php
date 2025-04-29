<?php

namespace Basis\Sharded\Interface;

interface Task
{
    public function __invoke(Database $database);
}
