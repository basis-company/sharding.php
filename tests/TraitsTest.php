<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Test\Entity\Post;
use PHPUnit\Framework\TestCase;

class TraitsTest extends TestCase
{
    public function testActiveRecord()
    {
        $database = new Database(new Runtime());
        $database->schema->register(Post::class);
        $post = $database->create(Post::class, ['name' => 'test']);
        $this->assertInstanceOf(Post::class, $post);

        $post->update('name', 'test2');
        $this->assertSame('test2', $post->name);

        $post->update(['name' => 'test3']);
        $this->assertSame('test3', $post->name);
        $post->delete();

        $this->assertCount(0, $database->find(Post::class));
    }
}
