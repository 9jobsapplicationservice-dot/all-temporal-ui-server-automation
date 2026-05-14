import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "RocketFlow Dashboard",
  description: "Workflow dashboard for LinkedIn apply, RocketReach enrichment, and email outreach.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full flex flex-col">{children}</body>
    </html>
  );
}
