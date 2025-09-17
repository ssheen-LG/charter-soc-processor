import { useEffect, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";

export default function ReportViewer() {
  const [reports, setReports] = useState<any[]>([]);
  const [index, setIndex] = useState(0);

  useEffect(() => {
    fetch("/soc_report.json")
      .then((res) => res.json())
      .then((data) => setReports(data))
      .catch((err) => console.error("Failed to load JSON:", err));
  }, []);

  const report = reports[index];

  if (!report) return <p className="p-6 text-white bg-slate-900 min-h-screen">Loading...</p>;

  const handlePrev = () => setIndex((prev) => Math.max(prev - 1, 0));
  const handleNext = () => setIndex((prev) => Math.min(prev + 1, reports.length - 1));

  return (
    <div className="min-h-screen w-full bg-slate-900 text-white p-6 space-y-6">
      <div className="flex justify-between items-center w-full">
        <h1 className="text-2xl font-bold text-cyan-300">{report.file_name}</h1>
        <div className="space-x-2">
          <button
            onClick={handlePrev}
            disabled={index === 0}
            className="px-4 py-2 border border-slate-600 rounded disabled:opacity-50 bg-slate-800 hover:bg-slate-700"
          >
            Previous
          </button>
          <button
            onClick={handleNext}
            disabled={index === reports.length - 1}
            className="px-4 py-2 border border-slate-600 rounded disabled:opacity-50 bg-slate-800 hover:bg-slate-700"
          >
            Next
          </button>
        </div>
      </div>

      <Card>
        <CardContent className="p-4 text-white bg-slate-800 border border-slate-600">
          <p><strong>Auditor:</strong> {report.ServiceAuditor}</p>
          <p><strong>Report Type:</strong> {report.SOC1ReportType}</p>
          <p><strong>Period:</strong> {report.ReportPeriod}</p>
          <p><strong>Opinion Date:</strong> {report.AuditorOpinionDate}</p>
        </CardContent>
      </Card>

      <Accordion type="multiple" className="space-y-2">

      <AccordionItem value="AuditorsOpinion">
          <AccordionTrigger>Auditor's Opinion</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-2">
              {report.AuditorOpinionType}
            </ul>
          </AccordionContent>
        </AccordionItem>
        <AccordionItem value="ThirdPartyServiceProvider">
          <AccordionTrigger>Third Party Service Providers</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-2">
              {report.ThirdPartyServiceProvider?.map((s: any, i: number) => (
                <li key={i}>
                  <strong>{s}</strong>
                </li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>
        <AccordionItem value="subservice">
          <AccordionTrigger>Subservice Providers</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6">
              {report.SubserviceProvider?.map((s: any, i: number) => (
                <li key={i}>{s}</li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>
        <AccordionItem value="services">
          <AccordionTrigger>Services Provided</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-2">
              {report.ServicesProvided?.map((s: any, i: number) => (
                <li key={i}>
                  <strong>{s.service}:</strong> {s.description}
                </li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>

        
        
        <AccordionItem value="control-numbers">
          <AccordionTrigger>Control Numbers</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-1">
              {report.ControlNumber?.map((obj: any, i: number) => (
                <li key={i}><strong>{obj}</strong></li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="control-objectives">
          <AccordionTrigger>Control Objectives</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-1">
              {report.ControlObjective?.map((obj: any, i: number) => (
                <li key={i}><strong>{obj.id}:</strong> {obj.objective}</li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="control-descriptions">
          <AccordionTrigger>Control Descriptions</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-1">
              {report.ControlDescription?.map((obj: any, i: number) => (
                <li key={i}><strong>{obj.number}:</strong> {obj.description}</li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="reports-inscope">
          <AccordionTrigger>Reports In Scope</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-1">
              {report.ReportsInScope?.map((r: any, i: number) => (
                <li key={i}>
                  <strong>{r.report_name}</strong> (Page {r.source_page}, Control: {r.source_control})
                </li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="control-exception">
          <AccordionTrigger>Control Exception Identified</AccordionTrigger>
          <AccordionContent>
            <p>{report.ControlExceptionIdentified}</p>
          </AccordionContent>
        </AccordionItem>

        <AccordionItem value="cuecs">
          <AccordionTrigger>CUECs</AccordionTrigger>
          <AccordionContent>
            <ul className="list-disc pl-6 space-y-1">
              {report.CUECDescription?.map((cuec: any, i: number) => (
                <li key={i}>
                  <strong>{cuec.number}:</strong> {cuec.description}
                </li>
              ))}
            </ul>
          </AccordionContent>
        </AccordionItem>

        
      </Accordion>
    </div>
  );
}
