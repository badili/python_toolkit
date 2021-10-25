from django.conf.urls import url
from django.urls import include, path, re_path

from toolkit import user_views as views

urlpatterns = [
    # user management
    url(r'^login$', views.login_page, name='login_page'),
    url(r'^logout/$', views.user_logout, name='user_logout'),
    re_path(r'^activate_user|deactivate_user/$', views.manage_users, name='manage_users'),
    re_path(r'^add_user/$', views.add_user, name='add_user'),
    # re_path(r'^(?P<d_type>edit_user)/$', views.edit_user, name='edit_user'),
    re_path(r'^activate_new_user/(?P<user>[0-9A-Za-z_\-]+)/(?P<token>[0-9A-Za-z_\:\-]{1,60})$', views.activate_user, name='activate'),
    re_path(r'^save_user_password$', views.save_user_password, name='save_user_password'),
    re_path(r'^new_user_password/?(?P<uid>[0-9A-Za-z_\-]+)?/?(?P<token>[0-9A-Za-z_\:\-]+)?$', views.new_user_password, name='new_user_password'),
    re_path(r'^recover_password$', views.recover_password, name='recover_password'),
    # user urls
]