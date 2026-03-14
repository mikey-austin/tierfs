import { render, screen } from '@testing-library/react';
import { describe, test, expect } from 'vitest';
import TierFSAdmin from './App';

describe('TierFSAdmin', () => {
  test('renders without crashing', () => {
    expect(() => render(<TierFSAdmin />)).not.toThrow();
  });

  test('displays navigation tabs', () => {
    render(<TierFSAdmin />);
    const tabs = ['Dashboard', 'Topology', 'Tiers', 'Files', 'Replication', 'Performance', 'Write Guard', 'Logs'];
    for (const tab of tabs) {
      expect(screen.getAllByText(tab).length).toBeGreaterThan(0);
    }
  });

  test('shows tier information', () => {
    render(<TierFSAdmin />);
    const tiers = ['tier0', 'tier1', 'tier2', 'tier3'];
    for (const tier of tiers) {
      expect(screen.getAllByText(tier).length).toBeGreaterThan(0);
    }
  });
});
