<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Test\Entity\Post;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class SelectTest extends TestCase
{
    public static function provideDrivers(): array
    {
        return [
            'tarantool' => [new Tarantool("tcp://" . getenv("TARANTOOL_HOST") . ":" . getenv("TARANTOOL_PORT"))],
            'runtime' => [new Runtime()],
        ];
    }

    #[DataProvider('provideDrivers')]
    public function testChanges(Driver $driver)
    {
        $database = new Database($driver->reset());
        $database->schema->register(Post::class);
        array_map(fn ($n) => $database->create(Post::class, ['name' => (string) $n]), range(1, 10));

        $this->assertCount(1, $database->driver->select('test_post')->limit(1));
        $this->assertCount(2, $database->driver->select('test_post')->limit(2));
        $this->assertCount(3, $database->driver->select('test_post')->limit(3));
        $posts = $database->driver->select('test_post')->limit(3)->toArray();
        $this->assertSame("3", array_pop($posts)->name);

        $posts = $database->driver->select('test_post')->where("id")->isGreaterThan(2)->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame("3", array_pop($posts)->name);

        $posts = $database->driver->select('test_post')->where("id")->isGreaterThan(2)->limit(2)->toArray();
        $this->assertCount(2, $posts);
        $this->assertSame(["3", "4"], array_map(fn ($post) => $post->name, $posts));
    }
}
