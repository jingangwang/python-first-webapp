import www.orm as orm
from www.models import User, Blog, Comment
import asyncio

loop = asyncio.get_event_loop()


async def test():
    await orm.create_pool(loop=loop, host='10.6.2.129', user='root', password='admin', db='test')
    user = User(name='Test2', email='test2@example.com', passwd='123456789', image='about:blank')
    await user.save()


loop.run_until_complete(test())


