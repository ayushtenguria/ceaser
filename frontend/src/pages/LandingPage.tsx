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
            <span>AI-Powered Data Analytics for B2B Teams</span>
          </div>
          <h1 className="mb-6 text-5xl font-bold leading-tight tracking-tight md:text-6xl">
            Stop waiting for data.
            <br />
            <span className="text-primary">Start asking questions.</span>
          </h1>
          <p className="mx-auto mb-10 max-w-2xl text-lg text-muted-foreground">
            Connect your database or upload Excel files. Ask questions in plain English.
            Get instant charts, tables, and AI-powered insights — no SQL, no coding, no data analyst needed.
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
                    Show me top 10 customers by revenue
                  </div>
                </div>
                <div className="flex gap-3">
                  <Bot className="mt-1 h-6 w-6 shrink-0 text-primary" />
                  <div className="rounded-2xl bg-secondary px-4 py-3 text-sm">
                    Here are your top 10 customers by total revenue. Meridian Financial leads with $812K,
                    followed by Citadel Defense at $737K...
                  </div>
                </div>
                <div className="ml-9 grid grid-cols-3 gap-3">
                  {["Meridian Financial — $812K", "Citadel Defense — $737K", "Quantum Mfg — $623K"].map((item) => (
                    <div key={item} className="rounded-lg border bg-card p-3 text-xs">
                      {item}
                    </div>
                  ))}
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
              icon={MessageSquare}
              title="Natural Language Queries"
              description="Ask questions in plain English. Our AI generates SQL or Python code, executes it, and returns results instantly."
            />
            <FeatureCard
              icon={LineChart}
              title="Smart Visualizations"
              description="Auto-selects the right chart type — bar, line, scatter, pie, histogram — based on your data patterns."
            />
            <FeatureCard
              icon={Brain}
              title="Data Analyst Agent"
              description="Ask strategic questions like 'how to increase revenue' — the AI runs multiple analyses and gives data-backed recommendations."
            />
            <FeatureCard
              icon={FileSpreadsheet}
              title="Excel Intelligence"
              description="Upload Excel files with multiple sheets. Auto-detects headers, relationships, data quality issues, and lets you query across sheets."
            />
            <FeatureCard
              icon={Globe}
              title="Cross-Database Queries"
              description="Query across multiple databases simultaneously. Perfect for microservice architectures where data lives in different DBs."
            />
            <FeatureCard
              icon={BookOpen}
              title="Reusable Notebooks"
              description="Save your analysis as a notebook. Run it again with different data — same analysis, fresh results, every time."
            />
            <FeatureCard
              icon={FileText}
              title="Automated Reports"
              description="Generate professional PDF reports from any conversation. Executive summary, charts, tables, and recommendations — all automated."
            />
            <FeatureCard
              icon={Layers}
              title="Semantic Layer"
              description="Define business metrics once — 'Revenue means subscription MRR only.' The AI uses your exact definitions, consistently."
            />
            <FeatureCard
              icon={Shield}
              title="Enterprise Security"
              description="Read-only queries enforced at 3 layers. Encrypted credentials. SOC2-ready audit logs. Role-based access control."
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
              question="How accurate are the SQL queries?"
              answer="Our 4-layer accuracy pipeline (semantic layer, SQL validation, result verification, and self-correcting repair agent) achieves 99%+ accuracy on production databases. You can always inspect the generated SQL."
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
              question="What's the difference between Ceaser and ChatGPT?"
              answer="ChatGPT can't connect to your database or execute queries. Ceaser connects directly to your data, runs real SQL/Python, and returns actual results from your database — not hallucinated answers."
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
          <h2 className="mb-4 text-3xl font-bold">Ready to talk to your data?</h2>
          <p className="mb-8 text-lg text-muted-foreground">
            Join teams who replaced their $10K/month data analysts with instant AI-powered insights.
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
