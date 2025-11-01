<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Tier;
use Basis\Sharding\Test\Entity\Activity;
use Basis\Sharding\Test\Entity\Post;
use PHPUnit\Framework\TestCase;
use TypeError;

class FactoryTest extends TestCase
{
    public function testChanges()
    {
        $database = new Database(new Runtime());
        $instances = [];

        $this->assertCount(2, $database->find(Bucket::class));

        $database->factory->afterCreate(function ($instance) use (&$instances) {
            if ($instance instanceof Post) {
                $instances[] = $instance;
            }
        });
        $database->schema->register(Post::class);
        $post = $database->create(Post::class, ['name' => 'tester']);
        $this->assertCount(1, $instances);
        $this->assertContains($post, $instances);

        $database->update($post, ['name' => 'tester2']);
        $this->assertSame($post->name, 'tester2');

        $database->factory->excludeFromIdentityMap(Activity::class);
        $database->schema->register(Activity::class);
        $activity = $database->create(Activity::class, ['name' => 'tester']);
        $this->assertCount(1, $database->find(Activity::class));
        $this->assertNotSame($database->find(Activity::class), $database->find(Activity::class));

        // without identity map properties should be also updated
        $database->update($activity, ['type' => 2]);
        $this->assertSame($activity->type, 2);

        $model = $database->schema->getModel(Tier::class);

        $database->factory->getInstance($model, [
            'name' => 'zz',
            'id' => 5,
        ]);

        $this->expectException(TypeError::class);

        $database->factory->setPropertySorter(false)->getInstance($model, [
            'name' => 'zz',
            'id' => 6,
        ]);
    }
}
