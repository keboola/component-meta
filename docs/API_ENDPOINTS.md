# Facebook Graph API Endpoints Reference

Complete reference of all endpoint combinations across Instagram Business Accounts, Facebook Ads Marketing API, and Facebook Pages Graph API.

**Last Updated:** 2026-01-28

---

## Table of Contents

1. [Instagram Business Accounts (via Facebook Login)](#1-instagram-business-accounts-via-facebook-login)
2. [Facebook Ads Marketing API](#2-facebook-ads-marketing-api)
3. [Facebook Pages Graph API](#3-facebook-pages-graph-api)
4. [Component Endpoint Patterns](#component-endpoint-patterns)

---

## 1. INSTAGRAM BUSINESS ACCOUNTS (via Facebook Login)

**Base URL:** `https://graph.facebook.com`
**Authentication:** Requires Facebook Page connection (current implementation)

### A. IG User Node Edges

#### Account Discovery
- `GET /me/accounts?fields=instagram_business_account` - Get IG accounts linked to FB Pages

#### Media Endpoints
- `GET /{ig-user-id}/media` - List all media (photos, videos, reels)
- `GET /{ig-media-id}` - Single media object metadata
- `GET /{ig-user-id}/stories` - Get stories

#### Comment Management
- `GET /{ig-media-id}/comments` - List comments on media
- `GET /{ig-comment-id}/replies` - Get comment replies
- `POST /{ig-media-id}/comments` - Post comment
- `DELETE /{ig-comment-id}` - Delete comment

#### Hashtag Search
- `GET /ig_hashtag_search` - Search hashtag by name
- `GET /{ig-hashtag-id}/top_media` - Top posts for hashtag
- `GET /{ig-hashtag-id}/recent_media` - Recent posts for hashtag
- `GET /{ig-user-id}/recently_searched_hashtags` - Last 7 days searches

#### User Fields
Available fields: `biography`, `id`, `followers_count`, `follows_count`, `media_count`, `name`, `profile_picture_url`, `username`, `website`

### B. IG Insights

#### Media-Level Insights
**Endpoint:** `GET /{ig-media-id}/insights`

**General Metrics:**
- `impressions` - Total times media was seen
- `reach` - Unique accounts that saw media
- `engagement` - Total interactions
- `saved` - Times media was saved
- `shares` - Times media was shared
- `video_views` - Video view count
- `plays` - Video plays
- `total_interactions` - All interactions

**Reel-Specific Metrics:**
- `clips_replays_count` - Times Reel was replayed
- `ig_reels_aggregated_all_plays_count` - Total Reel plays
- `ig_reels_avg_watch_time` - Average watch time
- `ig_reels_video_view_total_time` - Total watch time

**Story-Specific Metrics:**
- `taps_forward` - Taps to next story
- `taps_back` - Taps to previous story
- `exits` - Exits from story
- `replies` - Story replies

#### Account-Level Insights
**Endpoint:** `GET /{ig-user-id}/insights`

**General Metrics:**
- `impressions` - Total content impressions
- `reach` - Unique accounts reached
- `profile_views` - Profile visits
- `follower_count` - Follower growth
- `website_clicks` - Website clicks from profile
- `get_directions_clicks` - Directions clicks

**Audience Metrics:**
- `audience_city` - Follower cities
- `audience_country` - Follower countries
- `audience_gender_age` - Gender/age breakdown
- `audience_locale` - Follower locales
- `online_followers` - When followers are online

#### Recent Deprecations (v21+ - January 2025)
❌ **Deprecated Metrics:**
- `video_views` (non-Reels content)
- `email_contacts` (time series)
- `phone_call_clicks`
- `text_message_clicks`

---

## 2. FACEBOOK ADS MARKETING API

**Base URL:** `https://graph.facebook.com`

### A. Ads Hierarchy Nodes

#### Account Level
- `GET /act_{ad-account-id}` - Account details
- `GET /act_{ad-account-id}/campaigns` - List campaigns
- `GET /act_{ad-account-id}/adsets` - List adsets
- `GET /act_{ad-account-id}/ads` - List ads
- `GET /act_{ad-account-id}/adcreatives` - List ad creatives

#### Campaign Level
- `GET /{campaign-id}` - Campaign details
- `GET /{campaign-id}/adsets` - Adsets in campaign
- `GET /{campaign-id}/ads` - Ads in campaign

#### AdSet Level
- `GET /{adset-id}` - AdSet details
- `GET /{adset-id}/ads` - Ads in adset

#### Ad Level
- `GET /{ad-id}` - Ad details
- `GET /{ad-id}/adcreatives` - Ad creatives
- `GET /{ad-id}/previews` - Ad previews

### B. Ads Insights (Primary Performance Endpoint)

#### Insights Endpoints by Level
- `GET /act_{ad-account-id}/insights` - Account-level insights
- `GET /{campaign-id}/insights` - Campaign-level insights
- `GET /{adset-id}/insights` - AdSet-level insights
- `GET /{ad-id}/insights` - Ad-level insights

#### DSL Syntax Parameters

All parameters now supported after SUPPORT-14107 fix:

1. **`level`** - Aggregation level
   - Values: `ad`, `adset`, `campaign`, `account`

2. **`breakdowns`** - Dimension breakdowns
   - Values: `age`, `gender`, `country`, `region`, `publisher_platform`, `device_platform`, `placement`, `impression_device`, `product_id`

3. **`action_breakdowns`** - Action dimension breakdowns
   - Values: `action_type`, `action_target_id`, `action_destination`, `action_device`, `action_reaction`

4. **`date_preset`** - Predefined date ranges
   - Values: `today`, `yesterday`, `last_3d`, `last_7d`, `last_14d`, `last_28d`, `last_30d`, `last_90d`, `this_month`, `last_month`, `this_quarter`, `lifetime`

5. **`time_increment`** - Time granularity
   - Values: `1` (daily), `7` (weekly), `monthly`, `all_days`

6. **`action_attribution_windows`** - Attribution windows
   - Values: `1d_click`, `7d_click`, `28d_click`, `1d_view`
   - ❌ Deprecated (Jan 2026): `7d_view`, `28d_view`

7. **`action_report_time`** - Action reporting time
   - Values: `impression`, `conversion`, `mixed`

8. **`use_account_attribution_setting`** - Use account settings
   - Values: `true`, `false`

9. **`use_unified_attribution_setting`** - Use unified attribution
   - Values: `true`, `false`

10. **`filtering`** - Advanced filtering
    - Format: JSON array `[{"field":"...","operator":"...","value":[...]}]`

11. **`summary_action_breakdowns`** - Summary action breakdowns
    - Same values as `action_breakdowns`

12. **`product_id_limit`** - Product catalog limit
    - Format: Integer

13. **`sort`** - Sort results
    - Format: `field_direction` (e.g., `reach_descending`)

14. **`summary`** - Include summary row
    - Values: `true`, `false`

15. **`default_summary`** - Use default summary
    - Values: `true`, `false`

16. **`time_range`** - Custom date range
    - Format: `{"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}`

17. **`time_ranges`** - Multiple date ranges
    - Format: JSON array of time_range objects

18. **`fields`** - Metrics to return
    - Extracted from `{field1,field2,field3}` syntax

#### Available Insights Metrics (70+)

**Performance Metrics:**
- `impressions` - Total ad impressions
- `reach` - Unique users reached
- `frequency` - Average impressions per user
- `clicks` - Total clicks
- `ctr` - Click-through rate
- `cpc` - Cost per click
- `cpm` - Cost per 1000 impressions
- `cpp` - Cost per 1000 people reached

**Financial Metrics:**
- `spend` - Amount spent
- `account_currency` - Account currency
- `cost_per_action_type` - Cost per action type (array)
- `cost_per_conversion` - Cost per conversion (array)
- `cost_per_unique_action_type` - Cost per unique action type
- `cost_per_outbound_click` - Cost per outbound click (array)

**Conversion Metrics:**
- `actions` - All actions (array)
- `conversions` - Conversions (array)
- `conversion_values` - Conversion values (array)
- `purchase_roas` - Return on ad spend for purchases (array)
- `website_purchase_roas` - Website purchase ROAS (array)
- `action_values` - Action values (array)

**Engagement Metrics:**
- `post_engagement` - Total post engagements
- `page_engagement` - Total page engagements
- `link_clicks` - Link clicks
- `outbound_clicks` - Outbound clicks (array)
- `post_reactions` - Post reactions
- `post_saves` - Post saves
- `post_shares` - Post shares

**Video Metrics:**
- `video_play_actions` - Video plays (array)
- `video_avg_time_watched_actions` - Average watch time (array)
- `video_p25_watched_actions` - 25% watched (array)
- `video_p50_watched_actions` - 50% watched (array)
- `video_p75_watched_actions` - 75% watched (array)
- `video_p95_watched_actions` - 95% watched (array)
- `video_p100_watched_actions` - 100% watched (array)
- `video_thruplay_watched_actions` - ThruPlay watched (array)
- `video_10_sec_watched_actions` - 10 sec watched (array)
- `video_15_sec_watched_actions` - 15 sec watched (array)
- `video_30_sec_watched_actions` - 30 sec watched (array)

**Unique Metrics:**
- `unique_clicks` - Unique clicks
- `unique_ctr` - Unique click-through rate
- `unique_actions` - Unique actions (array)
- `unique_link_clicks_ctr` - Unique link CTR

**Other Metrics:**
- `social_spend` - Social spend
- `inline_link_clicks` - Inline link clicks
- `inline_post_engagement` - Inline post engagements
- `quality_score_organic` - Organic quality score
- `quality_score_ectr` - Expected CTR quality score
- `quality_score_ecvr` - Expected CVR quality score

#### Action Stats Fields

These fields create separate `_insights` tables in the component:

- `actions`
- `action_values`
- `conversions`
- `conversion_values`
- `cost_per_10_sec_video_view`
- `cost_per_action_type`
- `cost_per_unique_action_type`
- `cost_per_conversion`
- `cost_per_outbound_click`
- `unique_actions`
- `video_10_sec_watched_actions`
- `video_15_sec_watched_actions`
- `video_30_sec_watched_actions`
- `video_avg_pct_watched_actions`
- `video_avg_percent_watched_actions`
- `video_avg_sec_watched_actions`
- `video_avg_time_watched_actions`
- `video_complete_watched_actions`
- `video_p100_watched_actions`
- `video_p25_watched_actions`
- `video_p50_watched_actions`
- `video_p75_watched_actions`
- `video_p95_watched_actions`
- `website_ctr`
- `website_purchase_roas`
- `purchase_roas`
- `outbound_clicks`
- `video_play_actions`
- `video_thruplay_watched_actions`

---

## 3. FACEBOOK PAGES GRAPH API

**Base URL:** `https://graph.facebook.com`

### A. Page Node Edges

#### Account Discovery
- `GET /me/accounts` - User's pages with access tokens

#### Content Edges
- `GET /{page-id}/feed` - Page feed (includes tagged posts, check-ins)
- `GET /{page-id}/posts` - Page posts only (not tagged content)
- `GET /{page-id}/published_posts` - Published posts
- `GET /{page-id}/photos` - Photos
- `GET /{page-id}/videos` - Videos
- `GET /{page-id}/albums` - Photo albums
- `GET /{page-id}/events` - Events

#### Engagement Edges
- `GET /{post-id}/comments` - Post comments
- `GET /{post-id}/likes` - Post likes
- `GET /{post-id}/reactions` - Post reactions (like, love, haha, wow, sad, angry)
- `GET /{post-id}/shares` - Post shares

#### Page Information
**Endpoint:** `GET /{page-id}`

**Available Fields:**
- `id` - Page ID
- `name` - Page name
- `about` - About section
- `category` - Page category
- `description` - Page description
- `emails` - Contact emails
- `fan_count` - Total fans/likes
- `followers_count` - Total followers
- `link` - Page URL
- `phone` - Contact phone
- `username` - Page username
- `website` - Website URL
- `verification_status` - Verification status

### B. Page Insights

#### Page-Level Insights
**Endpoint:** `GET /{page-id}/insights`

**Impression Metrics:**
- `page_impressions` - Total page impressions
- `page_impressions_unique` - Unique page impressions
- `page_impressions_paid` - Paid impressions
- `page_impressions_organic` - Organic impressions
- `page_impressions_viral` - Viral impressions

**Engagement Metrics:**
- `page_engaged_users` - Users who engaged
- `page_post_engagements` - Post engagements
- `page_consumptions` - Content consumptions
- `page_negative_feedback` - Negative feedback

**Fan Metrics:**
- `page_fan_adds` - New fans
- `page_fan_removes` - Lost fans

**View Metrics:**
- `page_views_total` - Total page views
- `page_views_logged_in_unique` - Unique logged-in views

**Available Periods:**
- `day` - Daily metrics
- `week` - Weekly metrics
- `days_28` - 28-day metrics
- `lifetime` - Lifetime metrics

#### Post-Level Insights
**Endpoint:** `GET /{post-id}/insights`

**Impression Metrics:**
- `post_impressions` - Total post impressions
- `post_impressions_unique` - Unique post impressions
- `post_impressions_paid` - Paid impressions
- `post_impressions_organic` - Organic impressions
- `post_impressions_viral` - Viral impressions

**Engagement Metrics:**
- `post_engaged_users` - Users who engaged
- `post_clicks` - Post clicks
- `post_negative_feedback` - Negative feedback
- `post_reactions_by_type_total` - Reactions by type

**Video-Specific Metrics:**
- `post_video_views` - Video views
- `post_video_view_time` - Total view time
- `post_video_complete_views_30s` - 30-second complete views

---

## Component Endpoint Patterns

This component (`src/page_loader.py`, `src/output_parser.py`) supports four distinct query patterns:

### 1. Nested Queries (`nested-query` type)

**Pattern:** Any Facebook/Instagram Graph API edge with field expansion

**Example Configuration:**
```json
{
  "type": "nested-query",
  "query": {
    "path": "feed",
    "fields": "message,created_time,likes,comments{message,from}"
  }
}
```

**API Call:** `GET /{page-id}/feed?fields=message,created_time,likes,comments{message,from}`

### 2. Async Insights Queries (`async-insights-query` type)

**Pattern:** Facebook Ads Insights API with async job creation

**Example Configuration:**
```json
{
  "type": "async-insights-query",
  "query": {
    "parameters": "fields=ad_id,impressions,clicks&level=ad&date_preset=last_month"
  }
}
```

**API Call:**
1. `POST /act_{ad-account-id}/insights` (creates async job)
2. `GET /{job-id}` (polls for completion)
3. `GET /{job-id}/insights` (retrieves results)

### 3. Direct Insights via DSL (SUPPORT-14107 Fix)

**Pattern:** When `path=None` and `fields` starts with `"insights"`

**Condition in Code:** `if not query_config.path and fields.startswith("insights")`

**Example Configuration:**
```json
{
  "type": "nested-query",
  "query": {
    "path": null,
    "fields": "insights.level(ad).action_breakdowns(action_type).date_preset(last_3d).time_increment(1){ad_id,ad_name,spend}"
  }
}
```

**DSL Parsing:** Component extracts DSL parameters into query params:
- `insights.level(ad)` → `level=ad`
- `.action_breakdowns(action_type)` → `action_breakdowns=action_type`
- `.date_preset(last_3d)` → `date_preset=last_3d`
- `.time_increment(1)` → `time_increment=1`
- `{ad_id,ad_name,spend}` → `fields=ad_id,ad_name,spend,account_id`

**API Call:** `GET /act_{ad-account-id}/insights?level=ad&action_breakdowns=action_type&date_preset=last_3d&time_increment=1&fields=ad_id,ad_name,spend,account_id`

### 4. Nested Field Expansion with DSL

**Pattern:** When `path` is set (e.g., `ads`, `campaigns`)

**Condition in Code:** DSL parsing is SKIPPED when `path` is set

**Example Configuration:**
```json
{
  "type": "nested-query",
  "query": {
    "path": "ads",
    "fields": "insights.level(ad).breakdowns(publisher_platform){ad_id,impressions,spend}"
  }
}
```

**API Call:** `GET /act_{ad-account-id}/ads?fields=insights.level(ad).breakdowns(publisher_platform){ad_id,impressions,spend}`

**Note:** DSL syntax stays intact in the `fields` parameter - it is NOT parsed into separate query parameters.

---

## Critical Implementation Notes

### Path Condition Requirement

The condition `if not query_config.path and fields.startswith("insights")` in `src/page_loader.py:310` is **essential** and must not be removed.

**Why:**
- Facebook API has TWO different patterns for insights queries
- **Direct insights** (`/insights?level=ad&...`) - DSL must be parsed into query params
- **Nested field expansion** (`/ads?fields=insights.level(ad){...}`) - DSL must stay intact

**Without this condition:**
- Nested queries would break (7 production-only tables in testing)
- Facebook API would reject malformed requests

### Table Naming Logic

From `src/output_parser.py:425-446`:

1. **Insights queries** (direct or async): Add `_insights` suffix if not present
2. **Nested tables**: Append nested table name (e.g., `query_name_comments`)
3. **Action stats**: Append `_{field_name}_insights` for action breakdown queries

---

## References

- [Instagram Graph API: Complete Developer Guide for 2025](https://elfsight.com/blog/instagram-graph-api-complete-developer-guide-for-2025/)
- [Instagram API 2026: Complete Developer Guide](https://getlate.dev/blog/instagram-api)
- [Instagram Insights metrics and dimensions](https://docs.supermetrics.com/docs/instagram-insights-fields)
- [Facebook Ads API Guide from A to Z](https://blog.coupler.io/facebook-ads-api/)
- [Meta Ads API: Complete Guide for Advertisers and Developers (2025)](https://admanage.ai/blog/meta-ads-api)
- [Comprehensive Guide to the Facebook Ads Reporting API](https://magicbrief.com/post/comprehensive-guide-to-the-facebook-ads-reporting-api)
- [Facebook Graph API Guide](https://data365.co/blog/facebook-graph-api-alternative)
- [Facebook Graph: The Page Node](https://krbnite.github.io/Facebook-Graph-The-Page-Node/)
- [Meta Marketing API Official Documentation](https://developers.facebook.com/docs/marketing-api/)
- [Instagram Platform API Documentation](https://developers.facebook.com/docs/instagram-platform)

---

**Document Version:** 1.0
**Last Updated:** 2026-01-28
**Related Issues:** SUPPORT-14107
