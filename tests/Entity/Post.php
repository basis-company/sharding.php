<?php

namespace Basis\Sharded\Test\Entity;

use Basis\Sharded\Interface\Subdomain;

class Post implements Subdomain
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function getSubdomain(): string
    {
        return 'posts';
    }
}
