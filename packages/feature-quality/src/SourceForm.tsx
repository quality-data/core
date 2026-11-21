import { createSignal, For, Show, type Component } from "solid-js";
import { Modal } from "@knime-orchestador/feature-kit/ui";
import { QualityError, sourcesApi, type Driver, type Refusal, type SourceView } from "./api";
import { t } from "./i18n";
import { fieldHint, fieldLabel, missing, refusalText, sourceBody, validName } from "./sources";

/**
 * A source's form (decision 0036): its fields come from the driver catalog,
 * so a new engine needs no change here. Editing, the stored secrets show
 * empty and are kept unless written again; nothing secret ever comes back.
 */
export const SourceForm: Component<{
  drivers: Driver[];
  editing?: SourceView;
  onClose: () => void;
  onSaved: (message: string, name: string) => void;
}> = (props) => {
  const first = props.editing?.driver ?? props.drivers[0]?.id ?? "";
  const [name, setName] = createSignal(props.editing?.name ?? "");
  const [driverId, setDriverId] = createSignal(first);
  const [values, setValues] = createSignal<Record<string, string>>({
    ...(props.editing?.fields ?? {}),
  });
  const [refusals, setRefusals] = createSignal<Refusal[]>([]);
  const [failed, setFailed] = createSignal<string>();
  const [saving, setSaving] = createSignal(false);
  const driver = () => props.drivers.find((d) => d.id === driverId());
  const stored = () => props.editing?.secrets ?? [];
  const lacking = () => {
    const d = driver();
    return d ? missing(d, values(), stored()) : [];
  };
  const set = (key: string, value: string) => setValues({ ...values(), [key]: value });

  const save = async (e: Event) => {
    e.preventDefault();
    const d = driver();
    if (!d) return;
    setSaving(true);
    setRefusals([]);
    setFailed(undefined);
    try {
      await sourcesApi.put(name(), sourceBody(d, values()));
      props.onSaved(
        t(props.editing ? "quality:sources.updated" : "quality:sources.created", { name: name() }),
        name(),
      );
    } catch (err) {
      const body = err instanceof QualityError ? (err.body as { errors?: Refusal[] }) : undefined;
      if (body?.errors) setRefusals(body.errors);
      else setFailed(t("quality:common.saveError"));
    } finally {
      setSaving(false);
    }
  };

  return (
    <Modal
      open
      title={props.editing ? props.editing.name : t("quality:sources.new")}
      onClose={props.onClose}
    >
      <form class="qd-form" onSubmit={(e) => void save(e)}>
        <Show when={!props.editing}>
          <NameField name={name()} onInput={setName} />
        </Show>
        <label class="qd-field">
          <span>{t("quality:sources.driver")}</span>
          <select value={driverId()} onChange={(e) => setDriverId(e.currentTarget.value)}>
            <For each={props.drivers}>
              {(d) => (
                <option
                  value={d.id}
                >{`${d.id} · ${t(`quality:sources.engine.${d.engine}`)}`}</option>
              )}
            </For>
          </select>
        </label>
        <Show when={driver()}>
          {(d) => <DriverFields driver={d()} values={values()} stored={stored()} onInput={set} />}
        </Show>
        <Notices refusals={refusals()} failed={failed()} lacking={lacking()} />
        <div class="qd-actions">
          <button type="button" class="fk-btn" onClick={props.onClose}>
            {t("quality:common.cancel")}
          </button>
          <button
            type="submit"
            class="fk-btn fk-btn--primary"
            disabled={saving() || lacking().length > 0 || !validName(name())}
          >
            {t("quality:common.save")}
          </button>
        </div>
      </form>
    </Modal>
  );
};

const NameField: Component<{ name: string; onInput: (v: string) => void }> = (props) => (
  <label class="qd-field">
    <span>{t("quality:sources.name")} *</span>
    <input
      class="qd-mono"
      autocomplete="off"
      value={props.name}
      onInput={(e) => props.onInput(e.currentTarget.value.trim())}
    />
    <small classList={{ "qd-bad": !!props.name && !validName(props.name) }}>
      {t("quality:sources.nameHint")}
    </small>
  </label>
);

const Notices: Component<{ refusals: Refusal[]; failed?: string; lacking: string[] }> = (props) => (
  <>
    <Show when={props.lacking.length > 0}>
      <p class="qd-hint">
        {t("quality:sources.missing", { fields: props.lacking.map(fieldLabel).join(", ") })}
      </p>
    </Show>
    <For each={props.refusals}>{(r) => <p class="qd-error">{refusalText(r)}</p>}</For>
    <Show when={props.failed}>{(why) => <p class="qd-error">{why()}</p>}</Show>
  </>
);

/** A driver's fields, from the catalog; a secret shows empty, and says so when one is stored. */
const DriverFields: Component<{
  driver: Driver;
  values: Record<string, string>;
  stored: string[];
  onInput: (key: string, value: string) => void;
}> = (props) => (
  <For each={props.driver.fields}>
    {(f) => (
      <Show
        when={f.kind !== "flag"}
        fallback={
          <FlagField
            field={f.key}
            on={props.values[f.key] === "true"}
            onInput={(on) => props.onInput(f.key, on ? "true" : "")}
          />
        }
      >
        <label class="qd-field">
          <span>{fieldLabel(f.key) + (f.required ? " *" : "")}</span>
          <input
            type={f.kind === "secret" && f.key !== "user" ? "password" : "text"}
            inputMode={f.kind === "port" ? "numeric" : undefined}
            autocomplete="off"
            placeholder={
              f.kind === "port" && props.driver.default_port
                ? String(props.driver.default_port)
                : ""
            }
            value={f.kind === "secret" ? "" : (props.values[f.key] ?? "")}
            onInput={(e) => props.onInput(f.key, e.currentTarget.value)}
          />
          <Show when={f.kind === "secret" && props.stored.includes(f.key)}>
            <small>{t("quality:sources.secretKept")}</small>
          </Show>
          <Show when={fieldHint(f.key)}>{(hint) => <small>{hint()}</small>}</Show>
        </label>
      </Show>
    )}
  </For>
);

/** A flag of the driver's, off unless set; off, it is left out of the body. */
const FlagField: Component<{ field: string; on: boolean; onInput: (on: boolean) => void }> = (
  props,
) => (
  <label class="qd-field">
    <span>
      <input
        type="checkbox"
        checked={props.on}
        onChange={(e) => props.onInput(e.currentTarget.checked)}
      />{" "}
      {fieldLabel(props.field)}
    </span>
    <Show when={fieldHint(props.field)}>{(hint) => <small>{hint()}</small>}</Show>
  </label>
);
