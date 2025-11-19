CREATE OR REPLACE TABLE staging_data.news_with_symbols AS
SELECT 
    news.content_id, 
    news.pubDate,
    news.title, 
    news.summary, 
    news.url, 
    news.company_key, 
    company.company_name, 
    industry.industry_name,
    industry.sector_key, 
    sector.sector_name
FROM raw_data.news AS news
JOIN raw_data.company_info AS company
    ON news.company_key = company.company_symbol
JOIN raw_data.industry_info AS industry
    ON company.industry_key = industry.industry_key
JOIN raw_data.sector_info AS sector
    ON industry.sector_key = sector.sector_key;
