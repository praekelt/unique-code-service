from twisted.application.service import ServiceMaker

serviceMaker = ServiceMaker(
    'unique-service', 'unique_code_service.service',
    'RESTful service for managing unique codes.', 'unique-code-service')
