<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Interface\Driver;
use Basis\Sharding\Test\Entity\Post;
use Basis\Sharding\Test\Entity\Stage;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

class SelectTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function testChanges(Driver $driver)
    {
        $database = new Database($driver->reset());
        $database->schema->register(Post::class);
        array_map(fn ($n) => $database->create(Post::class, ['name' => (string) $n]), range(1, 10));

        $this->assertCount(1, $driver->select('test_post')->limit(1));
        $this->assertCount(2, $driver->select('test_post')->limit(2));
        $this->assertCount(3, $driver->select('test_post')->limit(3));
        $posts = $driver->select('test_post')->limit(3)->toArray();
        $this->assertSame("3", array_pop($posts)->name);

        $posts = $driver->select('test_post')->where('id')->isGreaterThan(2)->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame("3", array_pop($posts)->name);

        $posts = $driver->select('test_post')->where('id')->isGreaterThan(2)->limit(2)->toArray();
        $this->assertCount(2, $posts);
        $this->assertSame(["3", "4"], array_map(fn ($post) => $post->name, $posts));

        $posts = $driver->select('test_post')->where('id')->isLessThan(2)->desc('id')->limit(1)->toArray();
        $posts = $driver->select('test_post')->where('id')->isLessThan(2)->desc('id')->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame(["1"], array_map(fn ($post) => $post->name, $posts));

        $posts = $driver->select('test_post')->where('id')->isLessThan(3)->desc('id')->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame(["2"], array_map(fn ($post) => $post->name, $posts));

        $posts = $driver->select('test_post')->where('id')->equals(2)->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame("2", array_pop($posts)->name);

        $this->assertCount(1, $database->select(Post::class)->limit(1));
        $this->assertCount(2, $database->select(Post::class)->limit(2));
        $this->assertCount(3, $database->select(Post::class)->limit(3));
        $posts = $database->select(Post::class)->limit(3)->toArray();
        $this->assertSame("3", array_pop($posts)->name);
        $this->assertInstanceOf(Post::class, array_pop($posts));

        $posts = $database->select(Post::class)->where('id')->isGreaterThan(2)->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame("3", array_pop($posts)->name);


        $posts = $database->select(Post::class)->where('id')->isGreaterThan(2)->limit(2)->toArray();
        $this->assertCount(2, $posts);
        $this->assertSame(["3", "4"], array_map(fn ($post) => $post->name, $posts));

        $posts = $database->select(Post::class)->where('id')->equals(2)->limit(1)->toArray();
        $this->assertCount(1, $posts);
        $this->assertSame("2", array_pop($posts)->name);

        $database->schema->register(Stage::class);
        $database->create(Stage::class, ['year' => 2021]);
        $database->create(Stage::class, ['year' => 2022]);
        $database->create(Stage::class, ['year' => 2023]);

        [$post] = $database->select(Stage::class)->where('year')->isLessThan(2023)->desc('year')->limit(1)->toArray();
        $this->assertSame(2022, $post->year);
    }
}
