package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func cmdVolume(args []string) error {
	if len(args) < 2 || isHelpArg(args[1]) {
		fmt.Fprint(os.Stderr, volumeUsageText(filepath.Base(os.Args[0])))
		return nil
	}

	switch args[1] {
	case "create":
		return cmdWorkspaceCreate(args)
	case "import":
		return cmdWorkspaceImport(args)
	case "mount":
		return cmdMountArgs(args[2:])
	case "unmount":
		return cmdUnmountArgs(args[2:])
	case "save":
		return cmdVolumeSave(args[2:])
	case "list":
		return cmdWorkspaceList(args)
	case "fork":
		return cmdWorkspaceFork(args)
	case "show":
		return cmdWorkspaceInfo(volumeArgsWithSubcommand(args, "info"))
	case "info":
		return cmdWorkspaceInfo(args)
	case "clone":
		return cmdWorkspaceClone(args)
	case "delete":
		return cmdWorkspaceDelete(args)
	case "config":
		return cmdWorkspaceConfig(args)
	case "default":
		return cmdWorkspaceDefault(args)
	case "set-default":
		return cmdWorkspaceSetDefault(args)
	case "unset-default":
		return cmdWorkspaceUnsetDefault(args)
	default:
		return fmt.Errorf("unknown volume subcommand %q\n\n%s", args[1], volumeUsageText(filepath.Base(os.Args[0])))
	}
}

func volumeArgsWithSubcommand(args []string, subcommand string) []string {
	rewritten := append([]string(nil), args...)
	if len(rewritten) > 1 {
		rewritten[1] = subcommand
	}
	return rewritten
}

func volumeUsageText(bin string) string {
	return brandHeaderString() + fmt.Sprintf(`Usage:
  %s vol <subcommand>

Subcommands:
  create <volume>                           Create an empty volume
  import [--force] [--mount-at-source] <volume> <directory>
                                                Import a local directory into a volume
  mount [<volume> [directory]]              Mount a volume to a local folder
  unmount [--delete] [<volume|directory>]   Unmount a volume
  save [--timeout 2m] [--json] <volume|directory>
                                                Save and verify a mounted sync volume in Redis
  list                                      List volumes
  fork [source-volume] <new-volume>         Fork a volume from its current checkpoint
  show [volume]                             Show volume metadata
  delete [--no-confirmation] [volume]...    Delete volumes

These commands operate on the file-tree object formerly shown as a workspace.
Agent Workspace composition commands live under '%s ws'.
`, bin, bin)
}
