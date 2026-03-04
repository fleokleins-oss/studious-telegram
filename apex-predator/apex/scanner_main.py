import asyncio
from maestro_worker import MaestroWorker
from dotenv import load_dotenv

load_dotenv()

async def main():
    print("🩸 APEX PREDATOR NEO v666 INICIANDO EM TESTNET...")
    worker = MaestroWorker(None, None, {"max_per_cycle": 8.0})  # será injetado
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
