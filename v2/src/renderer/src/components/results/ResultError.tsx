export function ResultError({ message }: { message: string }) {
  return (
    <div className="result-error">
      <header>Query failed</header>
      <pre>{message}</pre>
    </div>
  );
}
