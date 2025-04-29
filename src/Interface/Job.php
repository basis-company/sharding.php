<?php

namespace Basis\Sharded\Interface;

interface Job
{
    public function __invoke(Database $database);
}
