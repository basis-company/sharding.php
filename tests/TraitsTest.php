<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Attribute\Reference;
use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Schema;
use Basis\Sharding\Test\Entity\Event;
use Basis\Sharding\Test\Entity\Post;
use Basis\Sharding\Test\Entity\PostTag;
use Basis\Sharding\Test\Entity\TagLabel;
use Basis\Sharding\Test\Entity\User;
use Exception;
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

    public function testReferences()
    {
        $database = new Database(new Runtime());
        $database->schema->register(Event::class);
        $database->schema->register(Post::class);
        $database->schema->register(PostTag::class);
        $database->schema->register(TagLabel::class);
        $database->schema->register(User::class);

        $nekufa = $database->create(User::class, ['name' => 'nekufa']);
        $post = $database->create(Post::class, ['name' => 'test', 'author' => $nekufa->id]);

        $this->assertSame($post->getAuthor(), $nekufa);
        $this->assertCount(1, $nekufa->getPostCollection());

        $bazyaba = $database->create(User::class, ['name' => 'bazyaba', 'parent' => $nekufa->id]);
        $this->assertCount(0, $bazyaba->getPostCollection());

        $database->create(Post::class, ['name' => 'test', 'author' => $bazyaba->id]);
        $database->create(Post::class, ['name' => 'test', 'author' => $bazyaba->id]);
        $last = $database->create(Post::class, [
            'name' => 'test',
            'author' => $bazyaba->id,
            'reviewer' => $nekufa->id,
            'administrator' => $bazyaba->id,
        ]);
        $this->assertCount(1, $nekufa->getPostCollection());
        $this->assertCount(3, $bazyaba->getPostCollection());

        $this->assertSame($last->getAuthor(), $bazyaba);
        $this->assertSame($last->getReviewer(), $nekufa);
        $this->assertSame($last->getAdministrator(), $bazyaba);

        try {
            $this->assertSame($bazyaba->getParent(), $nekufa);
            $this->assertNull("Method was called");
        } catch (Exception) {
        }

        // references without attributes
        $database->schema->addReference(Reference::create(User::class, 'parent', User::class));
        $this->assertSame($bazyaba->getParent(), $nekufa);

        $this->assertCount(0, $last->getPostTagCollection());

        $label = $database->create(TagLabel::class, ['label' => 'test']);

        $database->create(PostTag::class, [
            'post' => $last->id,
            'tagLabel' => $label->id,
        ]);

        $this->assertCount(1, $last->getPostTagCollection());
        $this->assertSame($last->getPostTagCollection()[0]->getTagLabel(), $label);
    }
}
