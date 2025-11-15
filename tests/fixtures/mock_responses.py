"""Mock Facebook API responses for testing."""

MOCK_ACCOUNTS_RESPONSE = {
    "data": [
        {
            "id": "123456789",
            "name": "Test Page 1",
            "category": "Software",
            "business_name": "Test Business",
            "access_token": "page_token_123"
        },
        {
            "id": "987654321",
            "name": "Test Page 2",
            "category": "Entertainment",
            "access_token": "page_token_456"
        }
    ],
    "paging": {
        "next": "https://graph.facebook.com/v23.0/me/accounts?after=cursor123"
    }
}

MOCK_ACCOUNTS_PAGE_2 = {
    "data": [
        {
            "id": "111222333",
            "name": "Test Page 3",
            "category": "Media",
            "access_token": "page_token_789"
        }
    ]
}

MOCK_ADACCOUNTS_RESPONSE = {
    "data": [
        {
            "account_id": "10152412627713995",
            "id": "act_10152412627713995",
            "name": "Test Ad Account",
            "currency": "USD",
            "business_name": "Test Business Inc"
        }
    ]
}

MOCK_IGACCOUNTS_RESPONSE = {
    "data": [
        {
            "id": "123456789",
            "name": "Test Page 1",
            "category": "Software",
            "instagram_business_account": {
                "id": "ig_111222333"
            }
        },
        {
            "id": "987654321",
            "name": "Test Page 2",
            "category": "Entertainment"
        }
    ]
}

MOCK_FEED_RESPONSE = {
    "data": [
        {
            "id": "post_123",
            "message": "Test post message",
            "caption": "Test caption",
            "created_time": "2024-01-01T12:00:00+0000",
            "type": "status",
            "description": "Test description",
            "comments": {
                "data": [
                    {
                        "id": "comment_456",
                        "message": "Great post!",
                        "created_time": "2024-01-01T13:00:00+0000",
                        "from": {
                            "id": "user_789",
                            "name": "Test User"
                        },
                        "likes": {
                            "data": [
                                {"id": "user_111", "name": "Liker 1", "username": "liker1"}
                            ]
                        },
                        "comments": {
                            "data": [
                                {
                                    "id": "subcomment_999",
                                    "message": "Reply to comment",
                                    "created_time": "2024-01-01T14:00:00+0000",
                                    "from": {"id": "user_222", "name": "Replier"}
                                }
                            ]
                        }
                    }
                ]
            },
            "likes": {
                "data": [
                    {"id": "user_333", "name": "Post Liker", "username": "postliker"}
                ]
            }
        }
    ],
    "paging": {}
}

MOCK_PAGE_INSIGHTS_RESPONSE = {
    "data": [
        {
            "id": "page_123/insights/page_fans/lifetime",
            "name": "page_fans",
            "period": "lifetime",
            "title": "Lifetime Total Likes",
            "description": "Lifetime total likes",
            "values": [
                {"value": 1000, "end_time": "2024-01-01T08:00:00+0000"}
            ]
        },
        {
            "id": "page_123/insights/page_impressions/day",
            "name": "page_impressions",
            "period": "day",
            "title": "Daily Total Impressions",
            "description": "Daily total impressions",
            "values": [
                {"value": 5000, "end_time": "2024-01-01T08:00:00+0000"},
                {"value": 5500, "end_time": "2024-01-02T08:00:00+0000"}
            ]
        }
    ]
}

MOCK_POST_INSIGHTS_RESPONSE = {
    "data": [
        {
            "id": "post_123",
            "insights": {
                "data": [
                    {
                        "id": "post_123/insights/post_impressions/lifetime",
                        "name": "post_impressions",
                        "period": "lifetime",
                        "values": [
                            {"value": 250}
                        ]
                    }
                ]
            }
        }
    ]
}

MOCK_ASYNC_JOB_START = {
    "report_run_id": "report_12345",
    "async_status": "Job Running",
    "async_percent_completion": 0
}

MOCK_ASYNC_JOB_RUNNING = {
    "id": "report_12345",
    "async_status": "Job Running",
    "async_percent_completion": 50
}

MOCK_ASYNC_JOB_COMPLETE = {
    "id": "report_12345",
    "async_status": "Job Completed",
    "async_percent_completion": 100
}

MOCK_ASYNC_INSIGHTS_RESULT = {
    "data": [
        {
            "account_id": "10152412627713995",
            "campaign_id": "campaign_111",
            "ad_id": "ad_222",
            "impressions": "10000",
            "clicks": "500",
            "spend": "250.50",
            "date_start": "2024-01-01",
            "date_stop": "2024-01-01",
            "actions": [
                {"action_type": "like", "value": "100"},
                {"action_type": "comment", "value": "25"},
                {"action_type": "post_reaction", "value": "50"}
            ]
        },
        {
            "account_id": "10152412627713995",
            "campaign_id": "campaign_111",
            "ad_id": "ad_333",
            "impressions": "8000",
            "clicks": "400",
            "spend": "200.00",
            "date_start": "2024-01-01",
            "date_stop": "2024-01-01",
            "actions": [
                {"action_type": "like", "value": "80"},
                {"action_type": "share", "value": "10"}
            ]
        }
    ]
}

MOCK_ASYNC_INSIGHTS_WITH_ACTION_BREAKDOWN = {
    "data": [
        {
            "account_id": "10152412627713995",
            "campaign_id": "campaign_111",
            "date_start": "2024-01-01",
            "date_stop": "2024-01-01",
            "actions": [
                {"action_type": "like", "action_reaction": "like", "value": "50"},
                {"action_type": "like", "action_reaction": "love", "value": "30"},
                {"action_type": "like", "action_reaction": "wow", "value": "20"}
            ]
        }
    ]
}

MOCK_ADS_RESPONSE = {
    "data": [
        {"id": "ad_111", "name": "Test Ad 1", "adset_id": "adset_222"},
        {"id": "ad_222", "name": "Test Ad 2", "adset_id": "adset_222"}
    ]
}

MOCK_CAMPAIGNS_RESPONSE = {
    "data": [
        {"id": "campaign_111", "name": "Test Campaign", "account_id": "act_10152412627713995"}
    ]
}

MOCK_ADSETS_RESPONSE = {
    "data": [
        {"id": "adset_222", "name": "Test Adset", "account_id": "act_10152412627713995"}
    ]
}

MOCK_BATCH_RESPONSE = {
    "act_10152412627713995": {
        "id": "act_10152412627713995",
        "name": "Account 1",
        "currency": "USD"
    },
    "act_20152412627713996": {
        "id": "act_20152412627713996",
        "name": "Account 2",
        "currency": "EUR"
    }
}

MOCK_FEED_WITH_SUMMARY = {
    "data": [
        {
            "id": "post_123",
            "message": "Test post",
            "created_time": "2024-01-01T12:00:00+0000",
            "likes": {
                "data": [],
                "summary": {
                    "total_count": 42,
                    "can_like": True
                }
            },
            "reactions": {
                "data": [],
                "summary": {
                    "total_count": 58
                }
            }
        }
    ]
}

MOCK_AD_WITH_SERIALIZED_LISTS = {
    "data": [
        {
            "id": "ad_123",
            "name": "Test Ad",
            "issues_info": [
                {"error_code": 1001, "error_message": "Test error"}
            ],
            "frequency_control_specs": [
                {"event": "impression", "interval_days": 7}
            ]
        }
    ]
}

MOCK_ERROR_PAGE_ACCESS_TOKEN = {
    "error": {
        "message": "This method must be called with a Page Access Token",
        "type": "OAuthException",
        "code": 190
    }
}

MOCK_ERROR_REDUCE_DATA = {
    "error": {
        "message": "Please reduce the amount of data you're asking for",
        "type": "OAuthException",
        "code": 1
    }
}
