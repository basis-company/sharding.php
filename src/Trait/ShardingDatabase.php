<?php

namespace Basis\Sharding\Trait;

use Basis\Sharding\Database;
use Exception;

trait ShardingDatabase
{
    private ?Database $shardingDatabase = null;

    public function setShardingDatabase(Database $database)
    {
        $this->shardingDatabase = $database;
    }

    public function getShardingDatabase(): Database
    {
        return $this->shardingDatabase ?: throw new Exception('Sharding database not set');
    }
}
