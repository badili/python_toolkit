import sys, inspect, importlib

from django.conf import settings
from django.contrib.auth import authenticate, login, logout, get_user_model
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.hashers import make_password
from django.contrib.sites.shortcuts import get_current_site
from django.core.exceptions import ValidationError
from django_registration.exceptions import ActivationError
from django_registration.backends.activation.views import RegistrationView, ActivationView
from django.shortcuts import render, redirect
from django.utils.encoding import force_bytes, force_text
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode
from django.urls import reverse
from django.contrib.auth.tokens import PasswordResetTokenGenerator
from sentry_sdk import init as sentry_init, capture_exception
from django.http import HttpResponse, JsonResponse, HttpResponseRedirect

# from rolepermissions.checkers import has_permission
# from rolepermissions.decorators import has_role_decorator
from rolepermissions.roles import get_user_roles, assign_role, remove_role, clear_roles
from rolepermissions.permissions import available_perm_names
# from rolepermissions.mixins import HasRoleMixin as has_role
# 
from datahub.settings.roles import Pi

from toolkit.terminal_output import Terminal
from vendor.notifications import Notification

terminal = Terminal()

class CustomPasswordResetTokenGenerator(PasswordResetTokenGenerator):
    """Custom Password Token Generator Class."""
    def _make_hash_value(self, user, timestamp):
        # Include user email alongside user password to the generated token
        # as the user state object that might change after a password reset
        # to produce a token that invalidated.
        login_timestamp = '' if user.last_login is None\
            else user.last_login.replace(microsecond=0, tzinfo=None)
        return str(user.pk) + user.password + user.email +\
            str(login_timestamp) + str(timestamp)


default_token_generator = CustomPasswordResetTokenGenerator()
sentry_init(settings.SENTRY_DSN, environment=settings.ENV_ROLE)

User = get_user_model()


def get_or_create_csrf_token(request):
    token = request.META.get('CSRF_COOKIE', None)
    if token is None:
        # Getting a new token
        token = csrf.get_token(request)
        request.META['CSRF_COOKIE'] = token

    request.META['CSRF_COOKIE_USED'] = True
    return token


def login_page(request, *args, **kwargs):
    csrf_token = get_or_create_csrf_token(request)
    page_settings = {'page_title': "%s | Login Page" % settings.SITE_NAME, 'csrf_token': csrf_token}

    try:
        # check if we have some username and password in kwargs
        # use the explicitly passed username and password over the form filled ones
        try:
            username = kwargs['user']['username']
            password = kwargs['user']['pass']
        except Exception:
            username = request.POST['username']
            password = request.POST['pass']    

        if 'message' in kwargs:
            page_settings['message'] = kwargs['message']

        if username is not None:
            user = authenticate(username=username, password=password)

            if user is None:
                terminal.tprint("Couldn't authenticate the user... redirect to login page", 'fail')
                page_settings['error'] = settings.SITE_NAME + " could not authenticate you. You entered an invalid username or password"
                page_settings['username'] = username
                return render(request, 'login.html', page_settings)
            else:
                terminal.tprint('All ok', 'debug')
                login(request, user)
                return redirect('/dashboard', request=request)
        else:
            return render(request, 'login.html', {username: username})
    except KeyError as e:
        if settings.DEBUG: capture_exception(e)
        # ask the user to enter the username and/or password
        terminal.tprint('\nUsername/password not defined: %s' % str(e), 'warn')
        page_settings['message'] = page_settings['message'] if 'message' in page_settings else "Please enter your username and password"
        return render(request, 'login.html', page_settings)
    except Profile.DoesNotExist as e:
        terminal.tprint(str(e), 'fail')
        # The user doesn't have a user profile, lets create a minimal one
        profile = Profile(
            user=user
        )
        profile.save()
        return render(request, 'login.html', page_settings)
    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        if settings.DEBUG: logging.error(traceback.format_exc())
        page_settings['message'] = "There was an error while authenticating you. Please try again and if the error persist, please contact the system administrator"
        return render(request, 'login.html', page_settings)


def user_logout(request):
    logout(request)
    # specifically clear this session variable
    if 'cur_user' in request.session:
        del request.session['cur_user']

    return redirect('/', request=request)


def update_password(uid, password, token):
    try:
        User = get_user_model()
        uuid = force_text(urlsafe_base64_decode(uid))
        try:
            user = User.objects.get(id=uuid)
        except ValueError: 
            user = User.objects.get(email=uuid)

        user.set_password(password)
        user.save()

        if user.check_password(password) == False:
            raise Exception('Your password has not been updated')
        
        # send an email that the account has been activated
        email_settings = {
            'template': 'emails/general-email.html',
            'subject': '[%s] Password Updated' % settings.SITE_NAME,
            'sender_email': settings.SENDER_EMAIL,
            'recipient_email': user.email,
            'use_queue': getattr(settings, 'QUEUE_EMAILS', False),
            'title': 'Password Updated',
            'message': 'Dear %s,<br /><p>You have successfully updated your password to the %s. You can now log in using your new password.</p>' % (user.first_name, settings.SITE_NAME),
        }
        notify = Notification()
        notify.send_email(email_settings)

        return user.email

    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        raise


def activate_user(request, user, token):
    try:
        uid = force_text(urlsafe_base64_decode(user))
        user = User.objects.get(pk=uid)

        activation_view = ActivationView()
        activation_view.validate_key(token)

        user.is_active = True
        user.save()

        # send an email that the account has been activated
        email_settings = {
            'template': 'emails/general-email.html',
            'subject': '[%s] Account Activated' % settings.SITE_NAME,
            'sender_email': settings.SENDER_EMAIL,
            'recipient_email': user.email,
            'use_queue': getattr(settings, 'QUEUE_EMAILS', False),
            'title': 'Account Activated',
            'message': 'Thank you for confirming your email. Your account at %s is now active.' % settings.SITE_NAME,
        }
        notify = Notification()
        notify.send_email(email_settings)
        
        uid = urlsafe_base64_encode(force_bytes(user.email))
        return HttpResponseRedirect('/new_user_password/%s/%s' % (uid, token))

    except ActivationError as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        return reverse('home', kwargs={'error': True, 'message': e.message})

    except User.DoesNotExist as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        return reverse('home', kwargs={'error': True, 'message': 'The specified user doesnt exist' })

    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        return reverse('home', kwargs={'error': True, 'message': 'There was an error while activating your account. Contact the system administrator' })


def new_user_password(request, uid=None, token=None):
    # the uid can be generated from a redirect when a user confirms their account
    # if not set, the user will have set their email on a user page
    current_site = get_current_site(request)
    params = {'site_name': settings.SITE_NAME, 'page_title': settings.SITE_NAME}
    
    # the uid is actually an encoded email
    try:
        if uid and token:
            # we have a user id and token, so we can present the new password page
            params['token'] = token
            params['user'] = uid
            return render(request, 'recover_password.html', params)
        # elif uid:
        #     uuid = force_text(urlsafe_base64_decode(uid))
        #     user = User.objects.get(id=uuid)
        else:
            # lets send an email with the reset link
            # print(request.POST.get('email'))
            user = User.objects.filter(email=request.POST.get('email')).get()
            notify = Notification()
            uid = urlsafe_base64_encode(force_bytes(user.pk))
            token = default_token_generator.make_token(user)
            email_settings = {
                'template': 'emails/verify_account.html',
                'subject': '[%s] Password Recovery Link' % settings.SITE_NAME,
                'sender_email': settings.SENDER_EMAIL,
                'recipient_email': user.email,
                'title': 'Password Recovery Link',
                'salutation': 'Dear %s' % user.first_name,
                'use_queue': getattr(settings, 'QUEUE_EMAILS', False),
                'verification_link': 'http://%s/new_user_password/%s/%s' % (current_site.domain, uid, token),
                'message': 'Someone, hopefully you tried to reset their password on %s. Please click on the link below to reset your password.' % settings.SITE_NAME,
                'message_sub_heading': 'Password Reset'
            }
            notify.send_email(email_settings)

        params['is_error'] = False
        params['message'] = 'We have sent a password recovery link to your email.'
        return render(request, 'login.html', params)

    except User.DoesNotExist as e:
        params['error'] = True
        capture_exception(e)
        params['message'] = 'Sorry, but the specified user is not found in our system'
        return render(request, 'new_password.html', params)

    except Exception as e:
        if settings.DEBUG: print(str(e))
        capture_exception(e)
        return render(request, 'login.html', {'is_error': True, 'message': 'There was an error while saving the new password'})


def save_user_password(request):
    # save the user password and redirect to the dashboard page
    try:
        passwd = request.POST.get('pass')
        repeat_passwd = request.POST.get('repeat_pass')

        if passwd != repeat_passwd:
            params['error'] = True
            params['message'] = "Sorry! Your passwords don't match. Please try again"
            params['token'] = request.POST.get('token')
            return render(request, 'new_user_password.html', params)

        username = update_password(request.POST.get('uuid'), passwd, request.POST.get('token'))

        # seems all is good, now login and return the dashboard
        return login_page(request, message='You have set a new password successfully. Please log in using the new password.', user={'pass': passwd, 'username': username})
    except Exception as e:
        if settings.DEBUG: print(str(e))
        capture_exception(e)
        return render(request, 'login.html', {'is_error': True, 'message': 'There was an error while saving the new password'})


def recover_password(request):
    current_site = get_current_site(request)
    # the uid is actually an encoded email
    params = {'site_name': settings.SITE_NAME, 'token': ''}

    return render(request, 'recover_password.html', params)


@login_required(login_url='/login')
def get_set_user_info(request):
    """ Get the basic information for the current logged in user
    """
    try:
        if request.session.session_key is not None:
            # update the session details
            User = get_user_model()
            cur_user = User.objects.get(id=request.user.id)
            clear_roles(cur_user)
            assign_role(cur_user, Pi)
            
            request.session['cu_designation'] = cur_user.get_designation_display()
            request.session['cu_designation_id'] = cur_user.designation
            request.session['cu_last_name'] = cur_user.last_name
            request.session['cu_first_name'] = cur_user.first_name
            request.session['cu_email'] = cur_user.email
            request.session['cu_issuperuser'] = cur_user.is_superuser
            if cur_user.is_superuser:
                request.session['cu_designation'] = 'Super Administrator'
            cur_user_email = cur_user.email

        elif request.session.get('cu_email') is not None:
            cur_user_email = request.session['cu_email']
        else:
            cur_user_email = None

        global user_permissions
        if cur_user_email: user_permissions = available_perm_names(cur_user)
        else: user_permissions = []

        if settings.DEBUG:
            if cur_user.is_superuser and len(user_permissions) == 0:
                user_permissions = ['all']

        return (cur_user_email, user_permissions)

    except User.DoesNotExist as e:
        terminal.tprint("%s: A user who is not vetted is trying to log in. Kick them out." % str(e), 'fail')
        return None

    except Exception as e:
        send_sentry_message(str(e))
        if settings.DEBUG: logging.error(traceback.format_exc())
        # we need a default page to go to
        return render(request, 'dashboard/dashboard.html', params)


@login_required(login_url='/login')
def update_user(request, user_id):
    try:
        UserModel = get_user_model()
        cur_user = User.objects.get(id=user_id)

        nickname=request.POST.get('username')
        username=request.POST.get('username')
        designation=request.POST.get('designation')
        tel=request.POST.get('tel')
        email=request.POST.get('email')
        first_name=request.POST.get('first_name')
        last_name=request.POST.get('surname')

        cur_user.nickname = username
        cur_user.username = username
        cur_user.designation = designation
        cur_user.tel = tel
        cur_user.email = email
        cur_user.first_name = first_name
        cur_user.last_name = last_name

        cur_user.full_clean()
        cur_user.save()
    
    except ValidationError as e:
        return {'error': True, 'message': 'There was an error while saving the user: %s' % str(e)}
    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        raise


@login_required(login_url='/login')
def manage_users(request, d_type):
    params = get_basic_info(request)
    u = User.objects.get(id=request.user.id)
    current_path = request.path.strip("/")

    try:
        # object_id = request.POST['object_id']
        object_id = my_hashids.decode(request.POST.get('object_id'))[0]

        if re.search('recipient|user$', current_path):
            cur_object = User.objects.filter(id=object_id).get()
        

        if re.search('^delete', current_path) and u.designation in super_users:
            cur_object.delete()
            return HttpResponse(json.dumps({'error': False, 'message': 'The %s has been deleted successfully' % re.search('_(.+)$', current_path).group(1)}))
        
        elif re.search('^deactivate', current_path) and u.designation in super_users:
            cur_object.is_active = not cur_object.is_active
            cur_object.save()
            to_return = json.dumps({'error': False, 'message': 'The %s has been updated successfully' % re.search('_(.+)$', current_path).group(1)})
        
        elif re.search('^activate', current_path) and u.designation in super_users:
            cur_object.is_active = not cur_object.is_active
            cur_object.save()
            to_return = json.dumps({'error': False, 'message': 'The %s has been updated successfully' % re.search('_(.+)$', current_path).group(1)})

        else:
            info_message = "Couldn't perform the requested action. Confirm if you have the proper permissions to conduct this action."
            sentry.captureMessage(info_message, level='info', extra={'user_designation': u.designation, 'request': current_path})
            return JsonResponse({'error': True, 'message': info_message}, safe=False)


        return HttpResponse(to_return)

    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        return HttpResponse(json.dumps({'error': True, 'message': 'There was an error while managing the %s' % re.search('_(.+)$', current_path).group(1)}))


@login_required(login_url='/login')
def edit_users(request, d_type):
    params = get_basic_info(request)

    try:
        if d_type == 'edit_user':
            dash_views.update_user(request, pk_id)

            # lets update the permissions too
            edit_user = User.objects.get(id=pk_id)
            clear_roles(edit_user)
            for name, obj in inspect.getmembers(importlib.import_module("coinfection.settings.roles"), inspect.isclass):
                if inspect.isclass(obj):
                    if name == 'AbstractUserRole': continue
                    if obj.alias == edit_user.designation:
                        assign_role(edit_user, obj)
                        break

            return JsonResponse({'error': False, 'message': 'The user has been edited successfully'})

    except DataError as e:
        transaction.rollback();
        if settings.DEBUG: terminal.tprint('%s (%s)' % (str(e), type(e)), 'fail')
        capture_exception(e)
        return JsonResponse({'error': True, 'message': 'Please check the entered data'})

    except Exception as e:
        if settings.DEBUG: terminal.tprint('%s (%s)' % (str(e), type(e)), 'fail')
        capture_exception(e)
        return JsonResponse({'error': True, 'message': 'There was an error while updating the database'})


@login_required(login_url='/login')
def add_user(request):
    # given a user details add the user
    # 1. Get the next personnel code
    # 1. Add the details of the user and set is_active to 0. Generate a password
    # 2. Send email to the user with the activation link
    
    try:
        UserModel = get_user_model()

        nickname=request.POST.get('username')
        username=request.POST.get('username')
        designation=request.POST.get('designation')
        tel=request.POST.get('tel')
        email=request.POST.get('email')
        first_name=request.POST.get('first_name')
        last_name=request.POST.get('surname')

        new_user = UserModel(
            nickname=username,
            username=username,
            designation=designation,
            tel=tel,
            email=email,
            first_name=first_name,
            last_name=last_name,
            password=make_password('TestPass1234'),
            is_active=0
        )
        new_user.full_clean()
        new_user.save()

        # assign roles
        # ToDo: Find a way to clean the roles import
        clear_roles(new_user)
        for name, obj in inspect.getmembers(importlib.import_module("datahub.settings.roles"), inspect.isclass):
            if inspect.isclass(obj):
                if name == 'AbstractUserRole': continue
                if obj.alias == new_user.designation:
                    assign_role(new_user, obj)
                    # print("The user %s is now a %s", (edit_user.username, obj.perm_name))
                    break

        reg_view = RegistrationView()
        activation_link = reg_view.get_activation_key(new_user)

        # send an email to this user
        notify = Notification()
        uid = urlsafe_base64_encode(force_bytes(new_user.pk))
        current_site = get_current_site(request)

        email_settings = {
            'template': 'emails/verify_account.html',
            'subject': '[%s] Confirm Registration' % settings.SITE_NAME,
            'sender_email': settings.SENDER_EMAIL,
            'recipient_email': email,
            'site_name': settings.SITE_NAME,
            'site_url': 'http://%s' % current_site.domain,
            'title': 'Confirm Registration',
            'salutation': 'Dear %s' % first_name,
            'use_queue': getattr(settings, 'QUEUE_EMAILS', False),
            'verification_link': 'http://%s/activate_new_user/%s/%s' % (current_site.domain, uid, activation_link),
            'message': 'You have been registered successfully to the %s. We are glad to have you on board. Please click on the button below to activate your account. You will not be able to use your account until it is activated. The activation link will expire in %d hours' % (settings.SITE_NAME, settings.ACCOUNT_ACTIVATION_DAYS * 24),
            'message_sub_heading': 'You have been registered successfully'
        }
        notify.send_email(email_settings)

        return JsonResponse({'error': False, 'message': 'The user has been saved successfully'})
    
    except ValidationError as e:
        return JsonResponse({'error': True, 'message': 'There was an error while saving the user: %s' % str(e)})
    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        raise


@login_required(login_url='/login')
def update_user(request, user_id):
    try:
        UserModel = get_user_model()
        cur_user = User.objects.get(id=user_id)

        nickname=request.POST.get('username')
        username=request.POST.get('username')
        designation=request.POST.get('designation')
        tel=request.POST.get('tel')
        email=request.POST.get('email')
        first_name=request.POST.get('first_name')
        last_name=request.POST.get('surname')

        cur_user.nickname = username
        cur_user.username = username
        cur_user.designation = designation
        cur_user.tel = tel
        cur_user.email = email
        cur_user.first_name = first_name
        cur_user.last_name = last_name

        cur_user.full_clean()
        cur_user.save()
    
    except ValidationError as e:
        return {'error': True, 'message': 'There was an error while saving the user: %s' % str(e)}
    except Exception as e:
        if settings.DEBUG: terminal.tprint(str(e), 'fail')
        capture_exception(e)
        raise