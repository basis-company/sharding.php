<?php

namespace Basis\Sharding\Test\Repository;

use Tarantool\Mapper\Repository;

class StageCorrection extends Repository
{
    public $engine = 'memtx';

    public $indexes = [
        [
            'fields' => ['id'],
            'unique' => true,
        ],
        [
            'fields' => ['stage'],
            'unique' => true,
        ],
    ];
}
