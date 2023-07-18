from django.conf import settings

from rest_framework import parsers, renderers
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from rest_framework.compat import coreapi, coreschema
from rest_framework.response import Response
from rest_framework.schemas import ManualSchema
from rest_framework.views import APIView

from raven import Client
from .terminal_output import Terminal

sentry = Client(settings.SENTRY_DSN)
terminal = Terminal()

class ObtainAuthToken(APIView):
    throttle_classes = ()
    permission_classes = ()
    parser_classes = (parsers.FormParser, parsers.MultiPartParser, parsers.JSONParser,)
    renderer_classes = (renderers.JSONRenderer,)
    serializer_class = AuthTokenSerializer
    if coreapi is not None and coreschema is not None:
        schema = ManualSchema(
            fields=[
                coreapi.Field(
                    name="username",
                    required=True,
                    location='form',
                    schema=coreschema.String(
                        title="Username",
                        description="Valid username for authentication",
                    ),
                ),
                coreapi.Field(
                    name="password",
                    required=True,
                    location='form',
                    schema=coreschema.String(
                        title="Password",
                        description="Valid password for authentication",
                    ),
                ),
            ],
            encoding="application/json",
        )

    def post(self, request, *args, **kwargs):
        serializer = self.serializer_class(data=request.data, context={'request': request})
        serializer.is_valid(raise_exception=True)
        user = serializer.validated_data['user']
        token, created = Token.objects.get_or_create(user=user)
        params = {'token': token.key, 'phone_no': user.tel, 'username': request.data['username']}

        return Response(params)


class PazuriAuthToken(ObtainAuthToken):
    def post(self, request, *args, **kwargs):
        try:
            serializer = self.serializer_class(data=request.data, context={'request': request})
            print(request.data)
            serializer.is_valid(raise_exception=True)
            user = serializer.validated_data['user']

            from common_func.registration import user_auth_details
            params = user_auth_details(user.id)
            
            return Response(params)

        except Exception as e:
            if settings.DEBUG: terminal.tprint(str(e), 'fail')
            sentry.captureException()
            return JsonResponse({'error': str(e)}, status=400, safe=False)


obtain_auth_token = ObtainAuthToken.as_view()
pazuri_auth_token = PazuriAuthToken.as_view()
