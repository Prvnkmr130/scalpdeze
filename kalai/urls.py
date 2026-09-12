from django.urls import path, include
from django.views.generic.base import RedirectView
from . import views

# Define the custom_admin namespace patterns
custom_admin_patterns = ([
    path("", RedirectView.as_view(url="/broker-admin/broker-login/"), name="index"),
    path("broker-login/", views.broker_login_view, name="broker_login"),
    path("callback/<str:account_id>/", views.broker_callback_view, name="broker_callback_account"),
    path("callback/", views.broker_callback_view, name="broker_callback"),
    path("fetch-remote-data/", views.fetch_remote_data_view, name="fetch_remote_data"),
    path("export-algo-logs/", views.export_algo_logs_view, name="export_algo_logs"),

], "custom_admin")

urlpatterns = [
    path("", views.home_view, name="home"),
    path("default-totp/", views.default_totp_view, name="default_totp_view"),
    path("fetch-totp/", views.fetch_totp_api, name="fetch_totp"),
    path("api/export/models/", views.export_models_api, name="export_models_api"),
    path("api/export/masters/", views.export_masters_api, name="export_masters_api"),
    path("api/export/pnl/", views.export_pnl_api, name="export_pnl_api"),
    path("api/export/ticks/", views.export_ticks_api, name="export_ticks_api"),
    path("api/export/algo-logs/", views.export_algo_logs_api, name="export_algo_logs_api"),
    path("api/data-hub/export/", views.export_data_endpoint, name="data_hub_export"),
    path("api/data-hub/clone/", views.clone_remote_data_api, name="data_hub_clone"),
    path("data-hub/", views.data_hub_view, name="data_hub_standalone"),
    path("api/deploy/git-info/", views.git_info_api, name="deploy_git_info"),
    path("api/deploy/trigger/", views.trigger_deploy_api, name="deploy_trigger"),
    path("api/deploy/status/", views.deploy_status_api, name="deploy_status"),
    path("deployment-hub/", views.deployment_hub_view, name="deployment_hub_standalone"),
    path("api/admin-hub/events/", views.admin_hub_events_api, name="admin_hub_events_api"),
    path("health/", views.health_check_view, name="health_check"),
    # Mount namespaced patterns at broker-admin/
    path("broker-admin/", include(custom_admin_patterns)),
]

#update comment.