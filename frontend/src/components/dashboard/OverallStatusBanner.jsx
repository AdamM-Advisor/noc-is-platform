import StatusBanner from '../ui/StatusBanner';
import { getStatusLevel } from '../ui/StatusDot';

export default function OverallStatusBanner({ status }) {
  if (!status) return null;

  const level = getStatusLevel(status.status);

  return (
    <StatusBanner
      status={level}
      title={status.status}
      narrative={status.narrative}
    />
  );
}
