<?php

namespace Basis\Sharded\Interface;

use Basis\Sharded\Database;
use Basis\Sharded\Entity\Bucket;
use Basis\Sharded\Entity\Storage;

interface Locator
{
    public static function castStorage(Database $database, Bucket $bucket): Storage;
}
