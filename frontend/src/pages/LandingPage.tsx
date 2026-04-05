import { useNavigate } from "react-router-dom";
import {
  BarChart3, Database, FileSpreadsheet, Bot, Sparkles, Shield,
  Zap, LineChart, Users, BookOpen, FileText, ChevronRight,
  Check, ArrowRight, MessageSquare, Layers, Brain, Globe,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export default function LandingPage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen bg-background text-foreground">
      {/* Navbar */}
      <nav className="fixed top-0 z-50 w-full border-b bg-background/80 backdrop-blur-sm">
        <div className="mx-auto flex h-16 max-w-6xl items-center justify-between px-6">
          <div className="flex items-center gap-2">
            <BarChart3 className="h-6 w-6 text-primary" />
            <span className="text-xl font-bold">Ceaser</span>
          </div>
          <div className="hidden items-center gap-8 md:flex">
            <a href="#features" className="text-sm text-muted-foreground hover:text-foreground transition-colors">Features</a>
            <a href="#how-it-works" className="text-sm text-muted-foreground hover:text-foreground transition-colors">How it Works</a>
            <a href="#pricing" className="text-sm text-muted-foreground hover:text-foreground transition-colors">Pricing</a>
            <a href="#faq" className="text-sm text-muted-foreground hover:text-foreground transition-colors">FAQ</a>
          </div>
          <div className="flex items-center gap-3">
            <Button variant="ghost" size="sm" onClick={() => navigate("/sign-in")}>
              Sign In
            </Button>
            <Button size="sm" onClick={() => navigate("/sign-up")}>
              Get Started Free
              <ArrowRight className="ml-1 h-4 w-4" />
            </Button>
          </div>
        </div>
      </nav>

      {/* Hero */}
      <section className="relative pt-32 pb-20 px-6">
        <div className="absolute inset-0 bg-gradient-to-b from-primary/5 to-transparent" />
        <div className="relative mx-auto max-w-4xl text-center">
          <div className="mb-6 inline-flex items-center gap-2 rounded-full border bg-card px-4 py-1.5 text-sm">
            <Sparkles className="h-4 w-4 text-primary" />
            <span>Smarter with every query — AI that learns your business</span>
          </div>
          <h1 className="mb-6 text-5xl font-bold leading-tight tracking-tight md:text-6xl">
            Your data analyst that
            <br />
            <span className="text-primary">never sleeps.</span>
          </h1>
          <p className="mx-auto mb-10 max-w-2xl text-lg text-muted-foreground">
            Connect databases, upload Excel files, or both. Ask questions in plain English.
            Get verified answers with confidence scores — not hallucinations.
            The AI remembers your corrections and gets more accurate with every query.
          </p>
          <div className="flex items-center justify-center gap-4">
            <Button size="lg" className="h-12 px-8 text-base" onClick={() => navigate("/sign-up")}>
              Start Free Trial
              <ArrowRight className="ml-2 h-5 w-5" />
            </Button>
            <Button variant="outline" size="lg" className="h-12 px-8 text-base" onClick={() => {
              document.getElementById("how-it-works")?.scrollIntoView({ behavior: "smooth" });
            }}>
              See How It Works
            </Button>
          </div>
          <p className="mt-4 text-sm text-muted-foreground">No credit card required · Free tier available</p>
        </div>

        {/* Hero visual */}
        <div className="mx-auto mt-16 max-w-5xl">
          <div className="rounded-xl border bg-card p-2 shadow-2xl">
            <div className="rounded-lg bg-muted/30 p-6">
              <div className="flex items-center gap-3 mb-4">
                <div className="h-3 w-3 rounded-full bg-red-500" />
                <div className="h-3 w-3 rounded-full bg-yellow-500" />
                <div className="h-3 w-3 rounded-full bg-green-500" />
                <span className="ml-4 text-sm text-muted-foreground">Ceaser — Chat with your data</span>
              </div>
              <div className="space-y-4">
                <div className="flex justify-end">
                  <div className="rounded-2xl bg-primary px-4 py-2 text-sm text-primary-foreground">
                    What's our MRR this month?
                  </div>
                </div>
                <div className="flex gap-3">
                  <Bot className="mt-1 h-6 w-6 shrink-0 text-primary" />
                  <div className="space-y-3 flex-1">
                    <div className="rounded-2xl bg-secondary px-4 py-3 text-sm">
                      Monthly Recurring Revenue is up 12.7% from last month, driven by 34 new Enterprise accounts.
                    </div>
                    {/* Metric Card */}
                    <div className="rounded-xl border bg-card p-5">
                      <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Monthly Recurring Revenue</p>
                      <div className="mt-2 flex items-baseline gap-3">
                        <span className="text-3xl font-bold">$54,300</span>
                        <span className="flex items-center gap-1 rounded-full bg-emerald-500/10 px-2 py-0.5 text-xs font-medium text-emerald-400">
                          <ChevronRight className="h-3 w-3 rotate-[-45deg]" />+12.7%
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-muted-foreground">Previous: $48,200</p>
                    </div>
                    {/* Confidence badge */}
                    <div className="flex items-center gap-1.5 rounded-md border border-emerald-500/20 bg-emerald-500/5 px-2 py-1 text-xs text-emerald-400">
                      <Shield className="h-3.5 w-3.5" />
                      <span className="font-medium">High confidence</span>
                      <span className="text-muted-foreground/70">— Verified query pattern</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Logos / Trust */}
      <section className="border-y bg-card/50 py-10 px-6">
        <div className="mx-auto max-w-4xl text-center">
          <p className="mb-6 text-sm text-muted-foreground">Trusted by data-driven teams</p>
          <div className="flex flex-wrap items-center justify-center gap-8 opacity-50">
            {["PostgreSQL", "MySQL", "BigQuery", "Snowflake", "Excel", "CSV"].map((name) => (
              <span key={name} className="text-lg font-semibold">{name}</span>
            ))}
          </div>
        </div>
      </section>

      {/* Features */}
      <section id="features" className="py-24 px-6">
        <div className="mx-auto max-w-6xl">
          <div className="mb-16 text-center">
            <h2 className="mb-4 text-3xl font-bold">Everything you need to analyze data</h2>
            <p className="mx-auto max-w-2xl text-muted-foreground">
              From natural language queries to automated reports — Ceaser is your complete data analytics platform.
            </p>
          </div>

          <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
            <FeatureCard
              icon={Shield}
              title="Verified Answers, Not Guesses"
              description="Every answer comes with a confidence score. High confidence? Trust it. Medium? Review the reasoning trail. The AI shows you WHY it chose those tables and joins."
            />
            <FeatureCard
              icon={Brain}
              title="Smarter With Every Query"
              description="Thumbs-down a wrong answer and it never makes the same mistake again. The AI learns your corrections, business terms, and metric definitions — permanently."
            />
            <FeatureCard
              icon={Layers}
              title="'Revenue' Means One Thing"
              description="Define metrics once, lock them. When 3 tables have 'revenue', Ceaser asks which one you mean — then remembers forever. No more inconsistent answers."
            />
            <FeatureCard
              icon={Zap}
              title="10M Rows in 2 Seconds"
              description="Powered by DuckDB under the hood. Queries run directly on parquet files — no loading into memory. Cross-file JOINs across uploads work instantly."
            />
            <FeatureCard
              icon={MessageSquare}
              title="Ask, Don't Code"
              description="Natural language to SQL or Python. The AI auto-selects the right approach, validates the query, and self-corrects errors — up to 3 repair attempts before giving up."
            />
            <FeatureCard
              icon={LineChart}
              title="KPI Cards & Smart Charts"
              description="Single-metric queries render as big number cards with trend arrows. Multi-value queries auto-select the right chart. No configuration needed."
            />
            <FeatureCard
              icon={FileSpreadsheet}
              title="Excel Files, Enterprise Scale"
              description="Upload CRM exports with 200+ columns. The AI selects only relevant columns, auto-corrects column name mismatches at runtime, and handles dirty data gracefully."
            />
            <FeatureCard
              icon={Globe}
              title="Cross-Source Intelligence"
              description="JOIN across databases, Excel files, and CSVs in one query. Upload your Salesforce export, connect your Postgres — query them together."
            />
            <FeatureCard
              icon={BookOpen}
              title="Notebooks & Reports"
              description="Save analyses as reusable notebooks. Generate PDF reports from any conversation. Schedule daily refreshes. Share with your team."
            />
          </div>
        </div>
      </section>

      {/* How it Works */}
      <section id="how-it-works" className="py-24 px-6 bg-card/30">
        <div className="mx-auto max-w-4xl">
          <div className="mb-16 text-center">
            <h2 className="mb-4 text-3xl font-bold">How it works</h2>
            <p className="text-muted-foreground">From connection to insight in 3 steps</p>
          </div>

          <div className="space-y-12">
            <Step
              number={1}
              title="Connect your data"
              description="Connect your PostgreSQL, MySQL, or SQLite database — or simply upload an Excel/CSV file. Ceaser auto-discovers your schema, relationships, and data quality."
              icon={Database}
            />
            <Step
              number={2}
              title="Ask questions"
              description="Type your question in plain English: 'Show me top customers by revenue' or 'What's our churn rate trend?' The AI generates and executes the right query automatically."
              icon={MessageSquare}
            />
            <Step
              number={3}
              title="Get insights"
              description="Receive instant tables, charts, and natural language insights. Save as reports, create reusable notebooks, or export to PDF — share with your team in seconds."
              icon={Zap}
            />
          </div>
        </div>
      </section>

      {/* Pricing */}
      <section id="pricing" className="py-24 px-6">
        <div className="mx-auto max-w-5xl">
          <div className="mb-16 text-center">
            <h2 className="mb-4 text-3xl font-bold">Simple, transparent pricing</h2>
            <p className="text-muted-foreground">Start free. Scale as you grow.</p>
          </div>

          <div className="grid gap-6 md:grid-cols-3">
            <PricingCard
              name="Free"
              price="$0"
              period=""
              description="For individuals exploring their data"
              features={[
                "1 user",
                "20 queries/day",
                "1 database connection",
                "5 file uploads/month",
                "Basic charts & tables",
              ]}
              cta="Get Started"
              onCta={() => navigate("/sign-up")}
            />
            <PricingCard
              name="Starter"
              price="$79"
              period="/month"
              description="For analysts and small teams"
              features={[
                "3 users",
                "100 queries/day",
                "3 database connections",
                "50 file uploads/month",
                "Notebooks & reports",
                "Analyst agent",
                "Email support",
              ]}
              highlighted
              cta="Start Free Trial"
              onCta={() => navigate("/sign-up")}
            />
            <PricingCard
              name="Business"
              price="$249"
              period="/month"
              description="For teams replacing their BI stack"
              features={[
                "10 users",
                "500 queries/day",
                "10 database connections",
                "Unlimited uploads",
                "Cross-DB queries",
                "Audit logs & API",
                "Priority support",
              ]}
              cta="Start Free Trial"
              onCta={() => navigate("/sign-up")}
            />
          </div>

          <p className="mt-8 text-center text-sm text-muted-foreground">
            Need more? <span className="text-foreground font-medium">Enterprise plans</span> start at $599/month with SSO, unlimited everything, and dedicated support.
          </p>
        </div>
      </section>

      {/* FAQ */}
      <section id="faq" className="py-24 px-6 bg-card/30">
        <div className="mx-auto max-w-3xl">
          <div className="mb-16 text-center">
            <h2 className="mb-4 text-3xl font-bold">Frequently asked questions</h2>
          </div>

          <div className="space-y-4">
            <FaqItem
              question="Can Ceaser modify or delete my data?"
              answer="No. Ceaser enforces read-only access at 3 layers: the AI prompt, a SQL validator, and the database connection itself (read-only transactions). It's physically impossible for Ceaser to modify your data."
            />
            <FaqItem
              question="What databases do you support?"
              answer="PostgreSQL, MySQL, and SQLite today. BigQuery and Snowflake connectors are coming soon. You can also upload Excel (.xlsx) and CSV files directly."
            />
            <FaqItem
              question="How accurate are the answers? Can I trust them?"
              answer="Every answer includes a confidence score (high/medium/low) and a reasoning trail showing WHY those tables and joins were chosen. Our 4-layer accuracy pipeline validates SQL before execution, verifies results after, and auto-repairs errors. Thumbs-up a correct answer and it becomes a verified pattern — reused instantly next time. Thumbs-down and the AI never makes that mistake again."
            />
            <FaqItem
              question="Is my data secure?"
              answer="Yes. Database credentials are encrypted at rest (AES-256). Query results stream directly to your browser and are not stored on our servers unless you save a report. We're SOC2-ready with full audit logging."
            />
            <FaqItem
              question="Can I use Ceaser with multiple databases?"
              answer="Yes. Business and Enterprise plans support cross-database queries — ask questions that span multiple databases and Ceaser joins the results automatically."
            />
            <FaqItem
              question="What's the difference between Ceaser and ChatGPT / Julius?"
              answer="ChatGPT can't connect to your database. Julius connects but lacks a semantic layer — 'revenue' means different things on different days. Ceaser lets you lock metric definitions, shows confidence scores on every answer, learns from your corrections, and handles 10M-row files via DuckDB without loading them into memory. Built for enterprise trust, not demo day."
            />
            <FaqItem
              question="Can it handle large files? My CRM export is 500K rows."
              answer="Yes. Ceaser uses DuckDB to query parquet files directly — no loading into memory. A GROUP BY on 10M rows takes 2 seconds. Cross-file JOINs across multiple uploads work the same way. Upload your 500K-row Salesforce export and start querying immediately."
            />
            <FaqItem
              question="Do I need to know SQL?"
              answer="No. That's the entire point. Ask questions in plain English and Ceaser handles the rest. Power users can inspect and edit the generated SQL if they want."
            />
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-24 px-6">
        <div className="mx-auto max-w-3xl text-center">
          <h2 className="mb-4 text-3xl font-bold">Your next data analyst doesn't need a salary.</h2>
          <p className="mb-8 text-lg text-muted-foreground">
            Answers in seconds, not days. Verified accuracy, not hallucinations.
            Starts at $0. No credit card. No setup.
          </p>
          <Button size="lg" className="h-12 px-8 text-base" onClick={() => navigate("/sign-up")}>
            Start Free — No Credit Card Required
            <ArrowRight className="ml-2 h-5 w-5" />
          </Button>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t bg-card py-12 px-6">
        <div className="mx-auto max-w-6xl">
          <div className="grid gap-8 md:grid-cols-4">
            <div>
              <div className="flex items-center gap-2 mb-4">
                <BarChart3 className="h-5 w-5 text-primary" />
                <span className="text-lg font-bold">Ceaser</span>
              </div>
              <p className="text-sm text-muted-foreground">
                AI-powered data analytics for B2B teams. Ask questions, get answers.
              </p>
            </div>
            <div>
              <h4 className="mb-3 text-sm font-semibold">Product</h4>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li><a href="#features" className="hover:text-foreground">Features</a></li>
                <li><a href="#pricing" className="hover:text-foreground">Pricing</a></li>
                <li><a href="#how-it-works" className="hover:text-foreground">How it Works</a></li>
                <li><a href="#faq" className="hover:text-foreground">FAQ</a></li>
              </ul>
            </div>
            <div>
              <h4 className="mb-3 text-sm font-semibold">Resources</h4>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li><a href="/setup" className="hover:text-foreground">Documentation</a></li>
                <li><a href="/changelog" className="hover:text-foreground">Changelog</a></li>
              </ul>
            </div>
            <div>
              <h4 className="mb-3 text-sm font-semibold">Company</h4>
              <ul className="space-y-2 text-sm text-muted-foreground">
                <li><a href="#how-it-works" className="hover:text-foreground">About</a></li>
                <li><a href="/privacy" className="hover:text-foreground">Privacy Policy</a></li>
                <li><a href="/terms" className="hover:text-foreground">Terms of Service</a></li>
                <li><a href="mailto:support@ceaser.ai" className="hover:text-foreground">Contact</a></li>
              </ul>
            </div>
          </div>
          <div className="mt-8 border-t pt-8 text-center text-sm text-muted-foreground">
            © {new Date().getFullYear()} Ceaser. All rights reserved.
          </div>
        </div>
      </footer>
    </div>
  );
}



function FeatureCard({ icon: Icon, title, description }: { icon: any; title: string; description: string }) {
  return (
    <div className="group rounded-xl border bg-card p-6 transition-all hover:border-primary/50 hover:shadow-lg">
      <div className="mb-4 flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10">
        <Icon className="h-5 w-5 text-primary" />
      </div>
      <h3 className="mb-2 text-lg font-semibold">{title}</h3>
      <p className="text-sm text-muted-foreground leading-relaxed">{description}</p>
    </div>
  );
}

function Step({ number, title, description, icon: Icon }: { number: number; title: string; description: string; icon: any }) {
  return (
    <div className="flex gap-6">
      <div className="flex flex-col items-center">
        <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-full bg-primary text-lg font-bold text-primary-foreground">
          {number}
        </div>
        {number < 3 && <div className="mt-2 h-full w-px bg-border" />}
      </div>
      <div className="pb-8">
        <div className="mb-2 flex items-center gap-2">
          <Icon className="h-5 w-5 text-primary" />
          <h3 className="text-xl font-semibold">{title}</h3>
        </div>
        <p className="text-muted-foreground leading-relaxed">{description}</p>
      </div>
    </div>
  );
}

function PricingCard({
  name, price, period, description, features, highlighted, cta, onCta,
}: {
  name: string; price: string; period: string; description: string;
  features: string[]; highlighted?: boolean; cta: string; onCta: () => void;
}) {
  return (
    <div className={cn(
      "rounded-xl border p-6 flex flex-col",
      highlighted ? "border-primary bg-primary/5 shadow-lg scale-105" : "bg-card",
    )}>
      {highlighted && (
        <div className="mb-4 inline-flex self-start rounded-full bg-primary px-3 py-1 text-xs font-medium text-primary-foreground">
          Most Popular
        </div>
      )}
      <h3 className="text-lg font-semibold">{name}</h3>
      <div className="mt-2 mb-1">
        <span className="text-4xl font-bold">{price}</span>
        <span className="text-muted-foreground">{period}</span>
      </div>
      <p className="mb-6 text-sm text-muted-foreground">{description}</p>
      <ul className="mb-8 flex-1 space-y-3">
        {features.map((f) => (
          <li key={f} className="flex items-center gap-2 text-sm">
            <Check className="h-4 w-4 shrink-0 text-primary" />
            {f}
          </li>
        ))}
      </ul>
      <Button
        className="w-full"
        variant={highlighted ? "default" : "outline"}
        onClick={onCta}
      >
        {cta}
      </Button>
    </div>
  );
}

function FaqItem({ question, answer }: { question: string; answer: string }) {
  return (
    <details className="group rounded-lg border bg-card">
      <summary className="flex cursor-pointer items-center justify-between p-4 text-sm font-medium">
        {question}
        <ChevronRight className="h-4 w-4 text-muted-foreground transition-transform group-open:rotate-90" />
      </summary>
      <div className="border-t px-4 py-3 text-sm text-muted-foreground leading-relaxed">
        {answer}
      </div>
    </details>
  );
}
