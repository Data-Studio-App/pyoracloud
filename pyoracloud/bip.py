import xml.etree.ElementTree as ET

try:
    from . import exceptions
    from . import env
except ImportError:
    import exceptions
    import env

SOAP_NS = "http://www.w3.org/2003/05/soap-envelope"
SCH_NS = "http://xmlns.oracle.com/oxp/service/ScheduleReportService"
NS_MAP = {"soap": SOAP_NS, "sch": SCH_NS}


class BipReport:
    def __init__(self, report_name: str, format: str = "csv") -> None:
        self.report_name = report_name
        self.format = format
        self.__params = []

    def add_param(self, name: str, value: str) -> None:
        self.__params.append((name, value))

    def get_report_request(self) -> ET.Element:
        report_request = ET.Element("reportRequest")

        attribute_format = ET.SubElement(report_request, "attributeFormat")
        attribute_format.text = self.format

        param_name_val = ET.SubElement(report_request, "parameterNameValue")
        for name, value in self.__params:
            list_param = ET.SubElement(param_name_val, "listOfParamNameValues")
            ET.SubElement(list_param, "name").text = name
            ET.SubElement(list_param, "values").text = value

        report_name = ET.SubElement(report_request, "reportAbsolutePath")
        report_name.text = self.report_name

        return report_request


class BipScheduler:
    def __init__(self, pod: env.Pod) -> None:
        self.pod = pod

    @property
    def schedule_report_url(self) -> str:
        uri: str = "xmlpserver/services/ScheduleReportWSSService"
        return f"{self.pod.url}/{uri}"

    def email(
        self,
        bip_rpt: BipReport,
        email_to: str,
        email_subject: str = None,
        email_attachment_name: str = None,
        email_body: str = None,
        email_cc: str = None,
        email_from: str = "noreply@oracle.com",
    ):
        if not email_subject:
            email_subject = f"{bip_rpt.report_name} Report"

        if not email_attachment_name:
            email_attachment_name = f"output.{bip_rpt.format}"

        email_options = ET.Element("emailOptions")
        ET.SubElement(email_options, "emailTo").text = email_to
        if email_cc:
            ET.SubElement(email_options, "emailCC").text = email_cc
        ET.SubElement(email_options, "emailFrom").text = email_from
        ET.SubElement(email_options, "emailSubject").text = email_subject
        ET.SubElement(email_options, "emailAttachmentName").text = email_attachment_name
        if email_body:
            ET.SubElement(email_options, "emailBody").text = email_body

        return self.run(bip_rpt, delivery_channel=email_options)

    def run(self, bip_rpt: BipReport, delivery_channel: ET.Element = None) -> str:

        self.pod.display_message(f"Submitting {bip_rpt.report_name}")
        self.pod.display_message(f"Url:  {self.schedule_report_url}")

        soap_envelope = ET.Element(ET.QName(SOAP_NS, "Envelope"))
        _ = ET.SubElement(soap_envelope, ET.QName(SOAP_NS, "Header"))
        soap_body = ET.SubElement(soap_envelope, ET.QName(SOAP_NS, "Body"))
        sch_rpt = ET.SubElement(soap_body, ET.QName(SCH_NS, "scheduleReport"))
        sch_rqst = ET.SubElement(sch_rpt, "scheduleRequest")
        delivery_channels = ET.SubElement(sch_rqst, "deliveryChannels")
        if delivery_channel:
            delivery_channels.append(delivery_channel)
        sch_rqst.append(bip_rpt.get_report_request())

        request = bip_rpt.pod.get_request()

        self.pod.display_message(f"Payload:  {ET.tostring(soap_envelope)}")
