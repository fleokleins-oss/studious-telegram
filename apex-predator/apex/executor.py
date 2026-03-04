import sys
import asyncio
print(f"🚀 Executor {sys.argv[2] if len(sys.argv) > 2 else 'default'} ONLINE")
asyncio.run(asyncio.sleep(999999))
