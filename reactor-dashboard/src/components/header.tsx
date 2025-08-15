import { ModeToggle } from "@/components/mode-toggle"
import { Button } from "@/components/ui/button"
import { SidebarTrigger } from "@/components/ui/sidebar"
import { Separator } from "@/components/ui/separator"
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label"


interface HeaderProps {
  pollingEnabled: boolean;
  setPollingEnabled: (value: boolean) => void;
  handleAddNode: () => void;
  pollOnce: () => void;
  POLL_MS: number;
  lastSweep: number | null; // timestamp in ms, or null if not available
}

export default function Header({
  pollingEnabled,
  setPollingEnabled,
  handleAddNode,
  pollOnce,
  POLL_MS,
  lastSweep,
}: HeaderProps) {
  return (
    <header className="flex h-(--header-height) shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-(--header-height)">
      <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
        <SidebarTrigger className="-ml-1" />
        <Separator
          orientation="vertical"
          className="mx-2 data-[orientation=vertical]:h-4"
        />
        <h1 className="text-xl font-bold">Reactor</h1>
        <div className="ml-auto flex items-center gap-2">
          <Badge variant="outline">
            Poll every {POLL_MS} ms
            {lastSweep && (
              <span className="ml-2">
                Last sweep: {new Date(lastSweep).toLocaleTimeString()}
              </span>
            )}
          </Badge>
          <Separator
            orientation="vertical"
            className="mx-2 data-[orientation=vertical]:h-4"
          />
          <Button onClick={handleAddNode} variant="default">
            + Add Node
          </Button>
          <Separator
              orientation="vertical"
              className="mx-2 data-[orientation=vertical]:h-4"
            />

          <Switch
            id="polling-toggle"
            checked={pollingEnabled}
            onCheckedChange={(checked) => setPollingEnabled(checked)}
          />
          <Label htmlFor="polling-toggle">Polling</Label>
          <Separator
              orientation="vertical"
              className="mx-2 data-[orientation=vertical]:h-4"
            />

          <Button onClick={pollOnce} variant="secondary">
            Refresh now
          </Button>

          <ModeToggle></ModeToggle>
        </div>

      <span className="text-sm text-gray-600">
        
      </span>
      </div>
    </header>
  );
}

