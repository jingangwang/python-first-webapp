from www.coroweb import get
from www.models import User


@get('/')
async def index():
    users = await User.find_all()
    return {
        '__template__': 'test.html',
        'users': users
    }
