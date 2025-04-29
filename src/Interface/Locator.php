<?php

namespace Basis\Sharding\Interface;

use Basis\Sharding\Database;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;

interface Locator
{
    public static function castStorage(Database $database, Bucket $bucket): Storage;
}
