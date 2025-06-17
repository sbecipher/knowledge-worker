from temporalio import workflow
import asyncio
from datetime import timedelta
from typing import List, Dict
from temporal_app.activities import list_company_articles, process_company_article, check_api_health

@workflow.defn
class KnowledgeWorkflow:
    @workflow.run
    async def run(self, companies: List[str], years: List[int]) -> Dict[str, list[dict]]:
        """
        Lists and processes articles for each company and year combination.
        First fetches metadata via list_company_articles, then processes each article.
        Returns a mapping of 'company_year' to list of processed article metadata.
        """
        # Ensure the external Knowledge API is reachable before fetching
        await workflow.execute_activity(
            check_api_health,
            start_to_close_timeout=timedelta(seconds=30),
        )
        result: Dict[str, list[dict]] = {}
        for company in companies:
            for year in years:
                key = f"{company}_{year}"
                # List article metadata via an activity
                articles_meta = await workflow.execute_activity(
                    list_company_articles,
                    args=[company, year],
                    start_to_close_timeout=timedelta(minutes=2),
                )
                # Process each article in parallel using asyncio.gather
                tasks = [
                    workflow.execute_activity(
                        process_company_article,
                        args=[company, year, meta],
                        start_to_close_timeout=timedelta(minutes=5),
                    )
                    for meta in articles_meta
                ]
                processed = []
                if tasks:
                    processed = await asyncio.gather(*tasks)
                result[key] = processed
        return result