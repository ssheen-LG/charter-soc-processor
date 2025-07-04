import React, { createContext, useContext, useState } from "react";

const AccordionItemContext = createContext<{ open: boolean; toggle: () => void } | undefined>(undefined);

export function Accordion({ type = "multiple", className = "", children }: any) {
  return <div className={`bg-slate-900 text-white ${className}`}>{children}</div>;
}

export function AccordionItem({ value, children }: { value: string; children: React.ReactNode }) {
  const [open, setOpen] = useState(false);
  const toggle = () => setOpen((prev) => !prev);

  return (
    <AccordionItemContext.Provider value={{ open, toggle }}>
      <div className="border border-slate-600 rounded-lg overflow-hidden bg-slate-800 text-white">
        {children}
      </div>
    </AccordionItemContext.Provider>
  );
}

export function AccordionTrigger({ children }: { children: React.ReactNode }) {
  const context = useContext(AccordionItemContext);
  if (!context) throw new Error("AccordionTrigger must be used within an AccordionItem");

  const { open, toggle } = context;

  return (
    <button
      onClick={toggle}
      className="w-full flex justify-between items-center p-4 text-left font-semibold bg-slate-700 hover:bg-slate-600 transition text-white border-b border-slate-600"
    >
      <span>{children}</span>
      <span>{open ? "−" : "+"}</span>
    </button>
  );
}

export function AccordionContent({ children }: { children: React.ReactNode }) {
  const context = useContext(AccordionItemContext);
  if (!context) throw new Error("AccordionContent must be used within an AccordionItem");

  return context.open ? (
    <div className="p-4 border-t border-slate-600 bg-slate-800 text-white">{children}</div>
  ) : null;
}
