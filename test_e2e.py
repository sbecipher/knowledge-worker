import asyncio
from temporalio.client import Client
from app.models.payloads import KnowledgeDocument


async def main():
    client = await Client.connect("localhost:7233")
    document = KnowledgeDocument(
        title="Alcoa Q1 2024 Earnings",
        company_name="Alcoa Corp",
        company_id="com_12345",
        company_ticker="AA",
        year=2024,
        url="https://investors.alcoa.com/financials/annual-reports-and-proxy-statements/default.aspx",
        type="html",
        filepath="alcoa_q1_2024.html",
        downloaded=False,
    )
    print("Executing workflow...")
    result = await client.execute_workflow(
        "KnowledgeIngestionWorkflow",
        document,
        id="test-workflow-7",
        task_queue="knowledge-ingestion-queue",
    )
    print(f"Workflow result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
