"""Sample configurations for testing, based on old component test configs."""

from configuration import Configuration, Account, QueryRow, QueryConfig

FEED_CONFIG_DICT = {
    "accounts": {
        "177057932317550": {
            "id": "177057932317550",
            "name": "keboola",
            "category": "software"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "feed",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "path": "feed",
                "fields": "caption,message,created_time,type,description,likes{name,username},comments{message,created_time,from,likes{name,username}}",
                "ids": "177057932317550",
                "since": "3 years ago",
                "until": "now"
            }
        }
    ]
}

ADS_CONFIG_DICT = {
    "accounts": {
        "act_10152412627713995": {
            "account_id": "10152412627713995",
            "business_name": "",
            "currency": "EUR",
            "id": "act_10152412627713995",
            "name": "blabla"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "ads",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "path": "ads",
                "fields": "id,name,adset_id",
                "ids": ""
            }
        },
        {
            "id": 2,
            "name": "campaigns",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "path": "campaigns",
                "fields": "id,name,account_id",
                "ids": ""
            }
        },
        {
            "id": 3,
            "name": "adsets",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "path": "adsets",
                "fields": "id,name,account_id",
                "ids": ""
            }
        }
    ]
}

RUNBYID_CONFIG_DICT = {
    "accounts": {
        "act_108176966036258": {
            "account_id": "108176966036258",
            "business_name": "account 2",
            "currency": "GBP",
            "id": "act_108176966036258",
            "name": "account 2"
        },
        "act_1146726535372240": {
            "account_id": "1146726535372240",
            "business_name": "",
            "currency": "GBP",
            "id": "act_1146726535372240",
            "name": "account 3"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "ads",
            "type": "nested-query",
            "run-by-id": True,
            "disabled": False,
            "query": {
                "path": "",
                "fields": "insights.time_range({\"since\":\"2019-09-29\",\"until\":\"2019-09-30\"}).level(ad).time_increment(1){account_id,account_name,ad_name,ad_id,impressions}",
                "ids": ""
            }
        }
    ]
}

ASYNC_INSIGHTS_CONFIG_DICT = {
    "accounts": {
        "act_522606278080331": {
            "account_id": "522606278080331",
            "business_name": "My bussiness",
            "currency": "CZK",
            "id": "act_522606278080331",
            "name": "My bussines"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "query",
            "type": "async-insights-query",
            "disabled": False,
            "query": {
                "parameters": "fields=account_id,campaign_id,actions&action_breakdowns=action_reaction&date_preset=last_3d&time_increment=1&level=account",
                "ids": ""
            }
        }
    ]
}

PAGE_INSIGHTS_CONFIG_DICT = {
    "accounts": {
        "177057932317550": {
            "id": "177057932317550",
            "name": "keboola",
            "category": "software"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "page",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "path": "",
                "fields": "insights.since(2 days ago).metric(page_views_by_age_gender_logged_in_unique,page_impressions_by_story_type,page_impressions_by_locale_unique,page_views_total,page_fans)",
                "ids": ""
            }
        }
    ]
}

POSTS_INSIGHTS_CONFIG_DICT = {
    "accounts": {
        "177057932317550": {
            "id": "177057932317550",
            "name": "keboola",
            "category": "software"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "posts_insights",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "limit": "40",
                "since": "24 months ago",
                "until": "now",
                "path": "feed",
                "fields": "insights.since(now).metric(post_video_view_time,post_engaged_fan,post_consumptions)",
                "ids": ""
            }
        }
    ]
}

FEED_SUMMARY_CONFIG_DICT = {
    "accounts": {
        "177057932317550": {
            "id": "177057932317550",
            "name": "keboola",
            "category": "software"
        }
    },
    "api-version": "v23.0",
    "queries": [
        {
            "id": 1,
            "name": "summarytest",
            "type": "nested-query",
            "disabled": False,
            "query": {
                "path": "",
                "fields": "posts.limit(10){id,created_time,message,likes.summary(true).limit(0),reactions.summary(total_count).limit(0)}",
                "ids": "177057932317550"
            }
        }
    ]
}


def create_test_configuration(config_dict: dict) -> Configuration:
    """Helper to create Configuration object from dict."""
    return Configuration(**config_dict)
